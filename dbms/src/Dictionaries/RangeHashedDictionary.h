#pragma once

#include <Dictionaries/IDictionary.h>
#include <Dictionaries/IDictionarySource.h>
#include <Dictionaries/DictionaryStructure.h>
#include <Common/HashTable/HashMap.h>
#include <Columns/ColumnString.h>
#include <ext/range.h>
#include <atomic>
#include <memory>
#include <tuple>


namespace DB
{

class RangeHashedDictionary final : public IDictionaryBase
{
public:
    RangeHashedDictionary(
        const std::string & name, const DictionaryStructure & dict_struct, DictionarySourcePtr source_ptr,
        const DictionaryLifetime dict_lifetime, bool require_nonempty);

    RangeHashedDictionary(const RangeHashedDictionary & other);

    std::exception_ptr getCreationException() const override { return creation_exception; }

    std::string getName() const override { return name; }

    std::string getTypeName() const override { return "RangeHashed"; }

    std::size_t getBytesAllocated() const override { return bytes_allocated; }

    std::size_t getQueryCount() const override { return query_count.load(std::memory_order_relaxed); }

    double getHitRate() const override { return 1.0; }

    std::size_t getElementCount() const override { return element_count; }

    double getLoadFactor() const override { return static_cast<double>(element_count) / bucket_count; }

    bool isCached() const override { return false; }

    DictionaryPtr clone() const override { return std::make_unique<RangeHashedDictionary>(*this); }

    const IDictionarySource * getSource() const override { return source_ptr.get(); }

    const DictionaryLifetime & getLifetime() const override { return dict_lifetime; }

    const DictionaryStructure & getStructure() const override { return dict_struct; }

    std::chrono::time_point<std::chrono::system_clock> getCreationTime() const override
    {
        return creation_time;
    }

    bool isInjective(const std::string & attribute_name) const override
    {
        return dict_struct.attributes[&getAttribute(attribute_name) - attributes.data()].injective;
    }

#define DECLARE_MULTIPLE_GETTER(TYPE)\
    void get##TYPE(\
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const PaddedPODArray<UInt16> & dates,\
        PaddedPODArray<TYPE> & out) const;
    DECLARE_MULTIPLE_GETTER(UInt8)
    DECLARE_MULTIPLE_GETTER(UInt16)
    DECLARE_MULTIPLE_GETTER(UInt32)
    DECLARE_MULTIPLE_GETTER(UInt64)
    DECLARE_MULTIPLE_GETTER(Int8)
    DECLARE_MULTIPLE_GETTER(Int16)
    DECLARE_MULTIPLE_GETTER(Int32)
    DECLARE_MULTIPLE_GETTER(Int64)
    DECLARE_MULTIPLE_GETTER(Float32)
    DECLARE_MULTIPLE_GETTER(Float64)
#undef DECLARE_MULTIPLE_GETTER

    void getString(
        const std::string & attribute_name, const PaddedPODArray<Key> & ids, const PaddedPODArray<UInt16> & dates,
        ColumnString * out) const;

    BlockInputStreamPtr getBlockInputStream(const Names & column_names, size_t max_block_size) const override;

private:
    struct Range : std::pair<UInt16, UInt16>
    {
        using std::pair<UInt16, UInt16>::pair;

        bool contains(const UInt16 date) const
        {
            const auto & left = first;
            const auto & right = second;

            if (left <= date && date <= right)
                return true;

            const auto has_left_bound = 0 < left && left <= DATE_LUT_MAX_DAY_NUM;
            const auto has_right_bound = 0 < right && right <= DATE_LUT_MAX_DAY_NUM;

            if ((!has_left_bound || left <= date) && (!has_right_bound || date <= right))
                return true;

            return false;
        }
    };

    template <typename T>
    struct Value final
    {
        Range range;
        T value;
    };

    template <typename T> using Values = std::vector<Value<T>>;
    template <typename T> using Collection = HashMap<UInt64, Values<T>>;
    template <typename T> using Ptr = std::unique_ptr<Collection<T>>;

    struct Attribute final
    {
    public:
        AttributeUnderlyingType type;
        std::tuple<UInt8, UInt16, UInt32, UInt64,
                   Int8, Int16, Int32, Int64,
                   Float32, Float64,
                   String> null_values;
        std::tuple<Ptr<UInt8>, Ptr<UInt16>, Ptr<UInt32>, Ptr<UInt64>,
                   Ptr<Int8>, Ptr<Int16>, Ptr<Int32>, Ptr<Int64>,
                   Ptr<Float32>, Ptr<Float64>, Ptr<StringRef>> maps;
        std::unique_ptr<Arena> string_arena;
    };

    void createAttributes();

    void loadData();

    template <typename T>
    void addAttributeSize(const Attribute & attribute);

    void calculateBytesAllocated();

    template <typename T>
    void createAttributeImpl(Attribute & attribute, const Field & null_value);

    Attribute createAttributeWithType(const AttributeUnderlyingType type, const Field & null_value);


    template <typename OutputType>
    void getItems(
        const Attribute & attribute,
        const PaddedPODArray<Key> & ids,
        const PaddedPODArray<UInt16> & dates,
        PaddedPODArray<OutputType> & out) const;

    template <typename AttributeType, typename OutputType>
    void getItemsImpl(
        const Attribute & attribute,
        const PaddedPODArray<Key> & ids,
        const PaddedPODArray<UInt16> & dates,
        PaddedPODArray<OutputType> & out) const;


    template <typename T>
    void setAttributeValueImpl(Attribute & attribute, const Key id, const Range & range, const T value);

    void setAttributeValue(Attribute & attribute, const Key id, const Range & range, const Field & value);

    const Attribute & getAttribute(const std::string & attribute_name) const;

    const Attribute & getAttributeWithType(const std::string & name, const AttributeUnderlyingType type) const;

    void getIdsAndDates(PaddedPODArray<Key> & ids,
                        PaddedPODArray<UInt16> & start_dates, PaddedPODArray<UInt16> & end_dates) const;

    template <typename T>
    void getIdsAndDates(const Attribute & attribute, PaddedPODArray<Key> & ids,
                        PaddedPODArray<UInt16> & start_dates, PaddedPODArray<UInt16> & end_dates) const;

    const std::string name;
    const DictionaryStructure dict_struct;
    const DictionarySourcePtr source_ptr;
    const DictionaryLifetime dict_lifetime;
    const bool require_nonempty;

    std::map<std::string, std::size_t> attribute_index_by_name;
    std::vector<Attribute> attributes;

    std::size_t bytes_allocated = 0;
    std::size_t element_count = 0;
    std::size_t bucket_count = 0;
    mutable std::atomic<std::size_t> query_count{0};

    std::chrono::time_point<std::chrono::system_clock> creation_time;

    std::exception_ptr creation_exception;
};

}
