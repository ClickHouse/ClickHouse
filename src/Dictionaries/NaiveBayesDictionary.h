#pragma once

#include <atomic>
#include <optional>
#include <variant>

#include <Columns/IColumn.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/NaiveBayesModel.h>
#include <Common/MapWithMemoryTracking.h>


namespace DB
{

/// The key is a text string, and the computed result is the predicted Naive Bayes class. The source
/// supplies the training data as rows of `(ngram String, class_id UInt32, count UInt64)`, and from it
/// the dictionary builds an immutable model once at load time.
///
/// A `dictGet` call for the class attribute classifies the input text. The dedicated `naiveBayesClassifier`
/// functions expose the same model and additionally return class probabilities, which `dictGet` cannot.
class NaiveBayesDictionary final : public IDictionary
{
public:
    struct Configuration
    {
        UInt32 n;
        TokenizerMode mode;

        double alpha;
        /// Padding tokens added at each end of the query input (n-1 each); empty means no padding (the default).
        /// Resolved to the bytes to pad with: a single byte for byte mode, the UTF-8 of a code point for
        /// codepoint mode, and the literal token for token mode.
        String start_token;
        String end_token;
        PriorsMode priors_mode;
        /// These probabilities are consulted only when the priors mode is explicit.
        MapWithMemoryTracking<UInt32, double> explicit_priors;
        /// When true, the source n-gram rows are retained so that the dictionary contents can be read back.
        bool store_source;
        /// Declared-attribute index of the class label and of the count, resolved from the `class_attribute` param.
        size_t class_index;
        size_t count_index;
        DictionaryLifetime dict_lifetime;
    };

    NaiveBayesDictionary(
        const StorageID & dict_id_,
        const DictionaryStructure & dict_struct_,
        DictionarySourcePtr source_ptr_,
        Configuration configuration_);

    std::string getTypeName() const override { return "NaiveBayes"; }

    size_t getBytesAllocated() const override { return bytes_allocated; }

    size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    /// Records `count` classified rows from the dedicated functions (e.g. naiveBayesClassifier), which bypass `getColumn` but should still
    /// be reflected in the dictionary query statistics.
    void incrementQueryCount(size_t count) const { query_count.fetch_add(count, std::memory_order_relaxed); }

    /// Every input is classifiable, so the found rate is one once any query has run.
    double getFoundRate() const override { return query_count.load(std::memory_order_relaxed) == 0 ? 0.0 : 1.0; }

    double getHitRate() const override { return 1.0; }

    size_t getElementCount() const override { return element_count; }

    /// The whole model is resident in memory, so the load factor is always one.
    double getLoadFactor() const override { return 1.0; }

    DictionarySourcePtr getSource() const override { return source_ptr; }

    const DictionaryLifetime & getLifetime() const override { return configuration.dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    bool isInjective(const std::string &) const override { return false; }

    DictionaryKeyType getKeyType() const override { return DictionaryKeyType::Complex; }

    std::shared_ptr<IExternalLoadable> clone() const override
    {
        return std::make_shared<NaiveBayesDictionary>(getDictionaryID(), dict_struct, source_ptr->clone(), configuration);
    }

    ColumnPtr getColumn(
        const std::string & attribute_name,
        const DataTypePtr & attribute_type,
        const Columns & key_columns,
        const DataTypes & key_types,
        DefaultOrFilter default_or_filter) const override;

    ColumnUInt8::Ptr hasKeys(const Columns & key_columns, const DataTypes & key_types) const override;

    Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const override;

    template <typename Func>
    decltype(auto) visitModel(Func && func) const
    {
        return std::visit(std::forward<Func>(func), *model_variant);
    }

private:
    using ModelVariant = std::variant<NaiveBayesModel<BytePolicy>, NaiveBayesModel<CodePointPolicy>, NaiveBayesModel<TokenPolicy>>;

    void loadData();

    DictionaryStructure dict_struct;
    DictionarySourcePtr source_ptr;
    Configuration configuration;

    /// This is populated once while the dictionary is being constructed and is never modified afterwards.
    std::optional<ModelVariant> model_variant;

    /// These hold the training rows and are populated only when the store source option is enabled.
    ColumnPtr source_ngram_column;
    ColumnPtr source_class_id_column;
    ColumnPtr source_count_column;

    size_t bytes_allocated = 0;
    size_t element_count = 0;

    mutable std::atomic<size_t> query_count{0};

    LoggerPtr log;
};

}
