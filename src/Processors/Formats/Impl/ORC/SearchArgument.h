#pragma once

#include <Processors/Formats/Impl/ORC/Proto.h>

#include <memory>
#include <string>
#include <vector>

/// Predicates for skipping stripes and row groups by the column statistics and the bloom filters.
/// The semantics (the truth values, the normalization of the expression, the evaluation of each
/// predicate against the statistics) are the same as in the reference implementation.

namespace DB::ORC
{

/// The possible outcomes of a predicate over a set of rows: whether any row can satisfy it (YES),
/// fail it (NO), or evaluate it to NULL.
enum class TruthValue : UInt8
{
    YES,
    NO,
    IS_NULL,
    YES_NULL,
    NO_NULL,
    YES_NO,
    YES_NO_NULL,
};

TruthValue operator||(TruthValue left, TruthValue right);
TruthValue operator&&(TruthValue left, TruthValue right);
TruthValue operator!(TruthValue value);
/// Whether the rows have to be read.
bool isNeeded(TruthValue value);

enum class PredicateDataType : UInt8
{
    LONG,
    FLOAT,
    STRING,
    DATE,
    DECIMAL,
    TIMESTAMP,
    BOOLEAN,
};

class Literal
{
public:
    struct Timestamp
    {
        Int64 second = 0;
        Int32 nanos = 0;

        Int64 getMillis() const { return second * 1000 + nanos / 1000000; }
        auto operator<=>(const Timestamp &) const = default;
    };

    struct Decimal
    {
        Int128 value = 0;
        Int32 scale = 0;

        /// Parses a decimal number like `-123.45`. Returns false if it is malformed.
        bool parse(std::string_view s);
        /// Without the trailing zeros in the fraction, as in the bloom filters.
        std::string toString() const;
    };

    explicit Literal(bool value);
    explicit Literal(Int64 value);
    explicit Literal(double value);
    Literal(const char * data, size_t size);
    /// Only for DATE: the number of days since the epoch.
    Literal(PredicateDataType type, Int64 value);
    /// TIMESTAMP: the seconds since the epoch in UTC and the nanoseconds.
    Literal(Int64 second, Int32 nanos);
    Literal(Int128 value, Int32 precision, Int32 scale);

    PredicateDataType getType() const { return type; }
    bool isNull() const { return is_null; }

    bool getBool() const { return int_value != 0; }
    Int64 getLong() const { return int_value; }
    Int64 getDate() const { return int_value; }
    double getFloat() const { return float_value; }
    const std::string & getString() const { return string_value; }
    Timestamp getTimestamp() const { return timestamp_value; }
    Decimal getDecimal() const { return decimal_value; }

private:
    PredicateDataType type;
    bool is_null = false;
    Int64 int_value = 0;
    double float_value = 0;
    std::string string_value;
    Timestamp timestamp_value;
    Decimal decimal_value;
};

class BloomFilter;

class PredicateLeaf
{
public:
    enum class Operator : UInt8
    {
        EQUALS,
        NULL_SAFE_EQUALS,
        LESS_THAN,
        LESS_THAN_EQUALS,
        IN,
        BETWEEN,
        IS_NULL,
    };

    PredicateLeaf(Operator op_, PredicateDataType type_, UInt64 column_id_, std::vector<Literal> literals_)
        : op(op_), type(type_), column_id(column_id_), literals(std::move(literals_)) {}

    UInt64 getColumnId() const { return column_id; }

    /// `writer_version` is from the postscript of the file. `bloom_filter` can be nullptr.
    TruthValue evaluate(UInt32 writer_version, const Proto::ColumnStatistics & stats, const BloomFilter * bloom_filter) const;

private:
    TruthValue evaluatePredicateMinMax(const Proto::ColumnStatistics & stats) const;
    TruthValue evaluatePredicateBloomFilter(const BloomFilter & bloom_filter, bool has_null) const;

    Operator op;
    PredicateDataType type;
    UInt64 column_id;
    std::vector<Literal> literals;
};

class ExpressionTree;
using ExpressionTreePtr = std::shared_ptr<ExpressionTree>;

class SearchArgument
{
public:
    SearchArgument(ExpressionTreePtr root_, std::vector<PredicateLeaf> leaves_);

    const std::vector<PredicateLeaf> & getLeaves() const { return leaves; }
    TruthValue evaluate(const std::vector<TruthValue> & leaf_values) const;

private:
    ExpressionTreePtr root;
    std::vector<PredicateLeaf> leaves;
};

class SearchArgumentBuilder
{
public:
    SearchArgumentBuilder();

    SearchArgumentBuilder & startOr();
    SearchArgumentBuilder & startAnd();
    SearchArgumentBuilder & startNot();
    SearchArgumentBuilder & end();

    SearchArgumentBuilder & literal(TruthValue value);
    SearchArgumentBuilder & lessThan(UInt64 column_id, PredicateDataType type, Literal value);
    SearchArgumentBuilder & lessThanEquals(UInt64 column_id, PredicateDataType type, Literal value);
    SearchArgumentBuilder & equals(UInt64 column_id, PredicateDataType type, Literal value);
    SearchArgumentBuilder & nullSafeEquals(UInt64 column_id, PredicateDataType type, Literal value);
    SearchArgumentBuilder & in(UInt64 column_id, PredicateDataType type, const std::vector<Literal> & values);
    SearchArgumentBuilder & isNull(UInt64 column_id, PredicateDataType type);
    SearchArgumentBuilder & between(UInt64 column_id, PredicateDataType type, Literal lower, Literal upper);

    std::unique_ptr<SearchArgument> build();

private:
    SearchArgumentBuilder & start(UInt8 op);
    SearchArgumentBuilder & addLeaf(PredicateLeaf leaf);

    std::vector<ExpressionTreePtr> stack;
    ExpressionTreePtr root;
    std::vector<PredicateLeaf> leaves;
};

/// A bloom filter of a row group (or a stripe), from the BLOOM_FILTER_UTF8 stream.
class BloomFilter
{
public:
    explicit BloomFilter(const Proto::BloomFilter & proto);

    bool testLong(Int64 value) const;
    bool testDouble(double value) const;
    bool testBytes(const char * data, size_t size) const;

private:
    bool testHash(Int64 hash) const;

    UInt32 num_hash_functions;
    std::vector<UInt64> bits;
};

}
