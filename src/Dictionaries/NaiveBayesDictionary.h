#pragma once

#include <atomic>
#include <map>
#include <optional>
#include <variant>

#include <Columns/IColumn.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/NaiveBayesModel.h>


namespace DB
{

/// A computational dictionary: the lookup key is an input to a computation rather than a stored key.
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
        String mode;                /// "byte", "codepoint", "token"
        double alpha;
        PriorsMode priors_mode;
        /// These probabilities are consulted only when the priors mode is explicit.
        std::map<UInt32, double> explicit_priors;
        /// When true, the source n-gram rows are retained so that the dictionary contents can be read back.
        bool store_source;
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

    double getFoundRate() const override
    {
        size_t queries = query_count.load(std::memory_order_relaxed);
        if (queries == 0)
            return 0;
        return static_cast<double>(found_count.load(std::memory_order_relaxed)) / static_cast<double>(queries);
    }

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

    ColumnUInt8::Ptr hasKeys(
        const Columns & key_columns,
        const DataTypes & key_types) const override;

    Pipe read(const Names & column_names, size_t max_block_size, size_t num_streams) const override;

    /// Invokes the given function with the concrete model, resolving the tokenizer-policy variant exactly
    /// once. Callers that classify many rows do so under a single call so that the per-row hot path holds no
    /// virtual or variant dispatch and can reuse one `NaiveBayesScratch`.
    template <typename Func>
    decltype(auto) visitModel(Func && func) const
    {
        return std::visit(std::forward<Func>(func), *classifier);
    }

private:
    using Classifier = std::variant<
        NaiveBayesModel<BytePolicy>,
        NaiveBayesModel<CodePointPolicy>,
        NaiveBayesModel<TokenPolicy>>;

    void loadData();

    DictionaryStructure dict_struct;
    DictionarySourcePtr source_ptr;
    Configuration configuration;

    /// This is populated once while the dictionary is being constructed and is never modified afterwards.
    std::optional<Classifier> classifier;

    /// These hold the training rows and are populated only when the store source option is enabled.
    ColumnPtr source_ngram_column;
    ColumnPtr source_class_id_column;
    ColumnPtr source_count_column;

    size_t bytes_allocated = 0;
    size_t element_count = 0;

    mutable std::atomic<size_t> query_count{0};
    mutable std::atomic<size_t> found_count{0};

    LoggerPtr log;
};

}
