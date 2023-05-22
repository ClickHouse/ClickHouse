#include <Columns/ColumnConst.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <IO/ReadBufferFromFile.h>
#include <Interpreters/Context.h>
#include <Common/ThreadPool.h>
#include <Common/setThreadName.h>
#include <Common/logger_useful.h>

#include <base/types.h>

#include <fmt/compile.h>
#include <fmt/format.h>


namespace DB
{
/** Ngram classifiers;
  *
  * ngramClassify(model, text) - classify text by naive Bayes classifier
  * Also support UTF8 formats.
  * ngramClassifyUTF8(model, text)
  */

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int BAD_ARGUMENTS;
}

template <typename BaysClassifierImpl, typename Name>
class NgramMostProbableClassifier : public IFunction
{
public:
    static constexpr auto name = Name::name;

    explicit NgramMostProbableClassifier(ContextPtr context_) : context(context_)
    {
        reloadClassifiers();
        reloading_thread = ThreadFromGlobalPool([this] { reloadClassifiersPeriodically(); });
    }

    ~NgramMostProbableClassifier() override
    {
        destroy.set();
        reloading_thread.join();
    }

    void reloadClassifiers()
    {
        const auto & config = context->getConfigRef();
        Poco::Util::AbstractConfiguration::Keys classifier_keys;
        config.keys(ngram_classifiers_config_key, classifier_keys);
        std::unordered_map<String, BaysClassifierImpl> new_ngram_classifiers(ngram_classifiers.size());

        for (auto & classifier_key : classifier_keys)
        {
            if (classifier_key.starts_with(ngram_classifier_config_key))
            {
                auto classifier_name = config.getString(
                    fmt::format(FMT_STRING("{}.{}.{}"), ngram_classifiers_config_key, classifier_key, ngram_classifier_name_config_key));

                const auto classes_key
                    = fmt::format(FMT_STRING("{}.{}.{}"), ngram_classifiers_config_key, classifier_key, ngram_classes_config_key);
                Poco::Util::AbstractConfiguration::Keys classes_keys;
                config.keys(classes_key, classes_keys);
                typename BaysClassifierImpl::NamedTexts named_texts;

                for (const auto & class_key : classes_keys)
                {
                    if (class_key.starts_with(ngram_class_config_key))
                    {
                        auto class_name = config.getString(fmt::format(
                            FMT_STRING("{}.{}.{}.{}.{}"),
                            ngram_classifiers_config_key,
                            classifier_key,
                            ngram_classes_config_key,
                            class_key,
                            ngram_class_name_config_key));
                        auto class_path = config.getString(fmt::format(
                            FMT_STRING("{}.{}.{}.{}.{}"),
                            ngram_classifiers_config_key,
                            classifier_key,
                            ngram_classes_config_key,
                            class_key,
                            ngram_class_path_config_key));

                        named_texts.emplace_back(std::move(class_name), readCorpus(class_path));
                    }
                }

                auto classifier = BaysClassifierImpl{};
                classifier.fitTexts(std::move(named_texts));
                new_ngram_classifiers.emplace(std::move(classifier_name), std::move(classifier));
            }
        }

        ngram_classifiers = std::move(new_ngram_classifiers);
    }

    static FunctionPtr create(ContextPtr context) { return std::make_shared<NgramMostProbableClassifier>(context); }

    String getName() const override { return name; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[0]->getName(), getName());

        if (!isString(arguments[1]))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Illegal type {} of argument of function {}", arguments[1]->getName(), getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & /*result_type*/, size_t /*input_rows_count*/) const override
    {
        using Probability = typename BaysClassifierImpl::Probability;

        const ColumnPtr & classifier_name_ptr = arguments[0].column;
        const ColumnConst * classifier_name_const = typeid_cast<const ColumnConst *>(&*classifier_name_ptr);
        if (!classifier_name_const)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Bad classifier name {}", arguments[1].column->getName());
        const String & classifier_name = classifier_name_const->getValue<String>();

        const auto & classifier_it = ngram_classifiers.find(classifier_name);
        if (classifier_it == ngram_classifiers.end())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no {} classifier", classifier_name);
        const auto & classifier = classifier_it->second;

        const ColumnPtr & column_ptr = arguments[1].column;
        const ColumnString * column_vector = checkAndGetColumn<ColumnString>(&*column_ptr);
        if (!column_vector)
        {
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Bad input column {} of function {}", arguments[1].column->getName(), getName());
        }

        auto result_column = ColumnString::create();

        const auto & column_chars = column_vector->getChars();
        const auto & offsets = column_vector->getOffsets();
        IColumn::Offset previous_offset = 0;

        for (size_t i = 0; i < column_vector->size(); previous_offset = offsets[i++])
        {
            const char * text = reinterpret_cast<const char *>(&column_chars[previous_offset]);
            IColumn::Offset size = offsets[i] - previous_offset - 1;

            auto probabilities = classifier.predict(text, size);

            String best_class_name;
            Probability best_probability = std::numeric_limits<Probability>::lowest();

            for (auto && [class_name, probability] : probabilities)
            {
                if (probability > best_probability)
                {
                    best_probability = probability;
                    best_class_name = std::move(class_name);
                }
            }

            result_column->insert(std::move(best_class_name));
        }

        return result_column;
    }

private:
    void reloadClassifiersPeriodically()
    {
        {
            auto thread_name = fmt::format(FMT_COMPILE("{}CorpusReload"), name);
            setThreadName(thread_name.data());
        }

        while (true)
        {
            if (destroy.tryWait(corpus_reload_period * 1000))
                return;

            LOG_TRACE(log, "Reloading ngram model");
            reloadClassifiers();
        }
    }

    String readCorpus(const String & path) const
    {
        ReadBufferFromFile in(path);

        String model;
        model.reserve((in.available()));
        readStringUntilEOF(model, in);

        in.close();
        return model;
    }

    static constexpr auto ngram_classifiers_config_key = "ngram_classifiers";
    static constexpr auto ngram_classifier_config_key = "ngram_classifier";
    static constexpr auto ngram_classifier_name_config_key = "ngram_classifier_name";
    static constexpr auto ngram_classes_config_key = "ngram_classes";
    static constexpr auto ngram_class_config_key = "ngram_class";
    static constexpr auto ngram_class_name_config_key = "ngram_class_name";
    static constexpr auto ngram_class_path_config_key = "ngram_class_path";

    static constexpr auto corpus_reload_period = 60; // in seconds

    std::unordered_map<String, BaysClassifierImpl> ngram_classifiers;
    ContextPtr context;
    ThreadFromGlobalPool reloading_thread;
    Poco::Event destroy;
    Poco::Logger * log = &Poco::Logger::get("NgramMostProbableClassifier");
};

}
