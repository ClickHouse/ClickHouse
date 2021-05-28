#include <Storages/MergeTree/MergeTreeIndexFullText.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/TreeRewriter.h>
#include <Interpreters/misc.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/RPNBuilder.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTSubquery.h>
#include <Core/Defines.h>

#include <Poco/Logger.h>

#include <boost/algorithm/string.hpp>

#if defined(__SSE2__)
#include <immintrin.h>

#if defined(__SSE4_2__)
#include <nmmintrin.h>
#endif

#endif


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int INCORRECT_QUERY;
    extern const int BAD_ARGUMENTS;
}


/// Adds all tokens from string to bloom filter.
static void stringToBloomFilter(
    const String & string, TokenExtractorPtr token_extractor, BloomFilter & bloom_filter)
{
    const char * data = string.data();
    size_t size = string.size();

    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;
    while (cur < size && token_extractor->nextInField(data, size, &cur, &token_start, &token_len))
        bloom_filter.add(data + token_start, token_len);
}

static void columnToBloomFilter(
    const char * data, size_t size, TokenExtractorPtr token_extractor, BloomFilter & bloom_filter)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;
    while (cur < size && token_extractor->nextInColumn(data, size, &cur, &token_start, &token_len))
        bloom_filter.add(data + token_start, token_len);
}


/// Adds all tokens from like pattern string to bloom filter. (Because like pattern can contain `\%` and `\_`.)
static void likeStringToBloomFilter(
    const String & data, TokenExtractorPtr token_extractor, BloomFilter & bloom_filter)
{
    size_t cur = 0;
    String token;
    while (cur < data.size() && token_extractor->nextLike(data, &cur, token))
        bloom_filter.add(token.c_str(), token.size());
}

/// Unified condition for equals, startsWith and endsWith
bool MergeTreeConditionFullText::createFunctionEqualsCondition(
    RPNElement & out, const Field & value, const BloomFilterParameters & params, TokenExtractorPtr token_extractor)
{
    out.function = RPNElement::FUNCTION_EQUALS;
    out.bloom_filter = std::make_unique<BloomFilter>(params);
    stringToBloomFilter(value.get<String>(), token_extractor, *out.bloom_filter);
    return true;
}

MergeTreeIndexGranuleFullText::MergeTreeIndexGranuleFullText(
    const String & index_name_,
    size_t columns_number,
    const BloomFilterParameters & params_)
    : index_name(index_name_)
    , params(params_)
    , bloom_filters(
        columns_number, BloomFilter(params))
    , has_elems(false)
{
}

void MergeTreeIndexGranuleFullText::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception("Attempt to write empty fulltext index " + backQuote(index_name), ErrorCodes::LOGICAL_ERROR);

    for (const auto & bloom_filter : bloom_filters)
        ostr.write(reinterpret_cast<const char *>(bloom_filter.getFilter().data()), params.filter_size);
}

void MergeTreeIndexGranuleFullText::deserializeBinary(ReadBuffer & istr)
{
    for (auto & bloom_filter : bloom_filters)
    {
        istr.read(reinterpret_cast<char *>(
                bloom_filter.getFilter().data()), params.filter_size);
    }
    has_elems = true;
}


MergeTreeIndexAggregatorFullText::MergeTreeIndexAggregatorFullText(
    const Names & index_columns_,
    const String & index_name_,
    const BloomFilterParameters & params_,
    TokenExtractorPtr token_extractor_)
    : index_columns(index_columns_)
    , index_name (index_name_)
    , params(params_)
    , token_extractor(token_extractor_)
    , granule(
        std::make_shared<MergeTreeIndexGranuleFullText>(
            index_name, index_columns.size(), params))
{
}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorFullText::getGranuleAndReset()
{
    auto new_granule = std::make_shared<MergeTreeIndexGranuleFullText>(
        index_name, index_columns.size(), params);
    new_granule.swap(granule);
    return new_granule;
}

void MergeTreeIndexAggregatorFullText::update(const Block & block, size_t * pos, size_t limit)
{
    if (*pos >= block.rows())
        throw Exception(
                "The provided position is not less than the number of block rows. Position: "
                + toString(*pos) + ", Block rows: " + toString(block.rows()) + ".", ErrorCodes::LOGICAL_ERROR);

    size_t rows_read = std::min(limit, block.rows() - *pos);

    for (size_t col = 0; col < index_columns.size(); ++col)
    {
        const auto & column = block.getByName(index_columns[col]).column;
        for (size_t i = 0; i < rows_read; ++i)
        {
            auto ref = column->getDataAt(*pos + i);
            columnToBloomFilter(ref.data, ref.size, token_extractor, granule->bloom_filters[col]);
        }
    }
    granule->has_elems = true;
    *pos += rows_read;
}


MergeTreeConditionFullText::MergeTreeConditionFullText(
    const SelectQueryInfo & query_info,
    ContextPtr context,
    const Block & index_sample_block,
    const BloomFilterParameters & params_,
    TokenExtractorPtr token_extactor_)
    : index_columns(index_sample_block.getNames())
    , index_data_types(index_sample_block.getNamesAndTypesList().getTypes())
    , params(params_)
    , token_extractor(token_extactor_)
    , prepared_sets(query_info.sets)
{
    rpn = std::move(
            RPNBuilder<RPNElement>(
                    query_info, context,
                    [this] (const ASTPtr & node, ContextPtr /* context */, Block & block_with_constants, RPNElement & out) -> bool
                    {
                        return this->atomFromAST(node, block_with_constants, out);
                    }).extractRPN());
}

bool MergeTreeConditionFullText::alwaysUnknownOrTrue() const
{
    /// Check like in KeyCondition.
    std::vector<bool> rpn_stack;

    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN
            || element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.push_back(true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS
             || element.function == RPNElement::FUNCTION_NOT_EQUALS
             || element.function == RPNElement::FUNCTION_IN
             || element.function == RPNElement::FUNCTION_NOT_IN
             || element.function == RPNElement::FUNCTION_MULTI_SEARCH
             || element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.push_back(false);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            // do nothing
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 && arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 || arg2;
        }
        else
            throw Exception("Unexpected function type in KeyCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    return rpn_stack[0];
}

bool MergeTreeConditionFullText::mayBeTrueOnGranule(MergeTreeIndexGranulePtr idx_granule) const
{
    std::shared_ptr<MergeTreeIndexGranuleFullText> granule
            = std::dynamic_pointer_cast<MergeTreeIndexGranuleFullText>(idx_granule);
    if (!granule)
        throw Exception(
                "BloomFilter index condition got a granule with the wrong type.", ErrorCodes::LOGICAL_ERROR);

    /// Check like in KeyCondition.
    std::vector<BoolMask> rpn_stack;
    for (const auto & element : rpn)
    {
        if (element.function == RPNElement::FUNCTION_UNKNOWN)
        {
            rpn_stack.emplace_back(true, true);
        }
        else if (element.function == RPNElement::FUNCTION_EQUALS
             || element.function == RPNElement::FUNCTION_NOT_EQUALS)
        {
            rpn_stack.emplace_back(granule->bloom_filters[element.key_column].contains(*element.bloom_filter), true);

            if (element.function == RPNElement::FUNCTION_NOT_EQUALS)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_IN
             || element.function == RPNElement::FUNCTION_NOT_IN)
        {
            std::vector<bool> result(element.set_bloom_filters.back().size(), true);

            for (size_t column = 0; column < element.set_key_position.size(); ++column)
            {
                const size_t key_idx = element.set_key_position[column];

                const auto & bloom_filters = element.set_bloom_filters[column];
                for (size_t row = 0; row < bloom_filters.size(); ++row)
                    result[row] = result[row] && granule->bloom_filters[key_idx].contains(bloom_filters[row]);
            }

            rpn_stack.emplace_back(
                    std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
            if (element.function == RPNElement::FUNCTION_NOT_IN)
                rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_MULTI_SEARCH)
        {
            std::vector<bool> result(element.set_bloom_filters.back().size(), true);

            const auto & bloom_filters = element.set_bloom_filters[0];

            for (size_t row = 0; row < bloom_filters.size(); ++row)
                result[row] = result[row] && granule->bloom_filters[element.key_column].contains(bloom_filters[row]);

            rpn_stack.emplace_back(
                    std::find(std::cbegin(result), std::cend(result), true) != std::end(result), true);
        }
        else if (element.function == RPNElement::FUNCTION_NOT)
        {
            rpn_stack.back() = !rpn_stack.back();
        }
        else if (element.function == RPNElement::FUNCTION_AND)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 & arg2;
        }
        else if (element.function == RPNElement::FUNCTION_OR)
        {
            auto arg1 = rpn_stack.back();
            rpn_stack.pop_back();
            auto arg2 = rpn_stack.back();
            rpn_stack.back() = arg1 | arg2;
        }
        else if (element.function == RPNElement::ALWAYS_FALSE)
        {
            rpn_stack.emplace_back(false, true);
        }
        else if (element.function == RPNElement::ALWAYS_TRUE)
        {
            rpn_stack.emplace_back(true, false);
        }
        else
            throw Exception("Unexpected function type in BloomFilterCondition::RPNElement", ErrorCodes::LOGICAL_ERROR);
    }

    if (rpn_stack.size() != 1)
        throw Exception("Unexpected stack size in BloomFilterCondition::mayBeTrueOnGranule", ErrorCodes::LOGICAL_ERROR);

    return rpn_stack[0].can_be_true;
}

bool MergeTreeConditionFullText::getKey(const ASTPtr & node, size_t & key_column_num)
{
    auto it = std::find(index_columns.begin(), index_columns.end(), node->getColumnName());
    if (it == index_columns.end())
        return false;

    key_column_num = static_cast<size_t>(it - index_columns.begin());
    return true;
}

bool MergeTreeConditionFullText::atomFromAST(
    const ASTPtr & node, Block & block_with_constants, RPNElement & out)
{
    Field const_value;
    DataTypePtr const_type;
    if (const auto * func = typeid_cast<const ASTFunction *>(node.get()))
    {
        const ASTs & args = typeid_cast<const ASTExpressionList &>(*func->arguments).children;

        if (args.size() != 2)
            return false;

        size_t key_arg_pos;           /// Position of argument with key column (non-const argument)
        size_t key_column_num = -1;   /// Number of a key column (inside key_column_names array)
        const auto & func_name = func->name;

        if (functionIsInOrGlobalInOperator(func_name) && tryPrepareSetBloomFilter(args, out))
        {
            key_arg_pos = 0;
        }
        else if (KeyCondition::getConstant(args[1], block_with_constants, const_value, const_type) && getKey(args[0], key_column_num))
        {
            key_arg_pos = 0;
        }
        else if (KeyCondition::getConstant(args[0], block_with_constants, const_value, const_type) && getKey(args[1], key_column_num))
        {
            key_arg_pos = 1;
        }
        else
            return false;

        if (const_type && const_type->getTypeId() != TypeIndex::String
                       && const_type->getTypeId() != TypeIndex::FixedString
                       && const_type->getTypeId() != TypeIndex::Array)
        {
            return false;
        }

        if (key_arg_pos == 1 && (func_name != "equals" && func_name != "notEquals"))
            return false;
        else if (!token_extractor->supportLike() && (func_name == "like" || func_name == "notLike"))
            return false;

        if (func_name == "notEquals")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_NOT_EQUALS;
            out.bloom_filter = std::make_unique<BloomFilter>(params);
            stringToBloomFilter(const_value.get<String>(), token_extractor, *out.bloom_filter);
            return true;
        }
        else if (func_name == "equals")
        {
            out.key_column = key_column_num;
            return createFunctionEqualsCondition(out, const_value, params, token_extractor);
        }
        else if (func_name == "like")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_EQUALS;
            out.bloom_filter = std::make_unique<BloomFilter>(params);
            likeStringToBloomFilter(const_value.get<String>(), token_extractor, *out.bloom_filter);
            return true;
        }
        else if (func_name == "notLike")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_NOT_EQUALS;
            out.bloom_filter = std::make_unique<BloomFilter>(params);
            likeStringToBloomFilter(const_value.get<String>(), token_extractor, *out.bloom_filter);
            return true;
        }
        else if (func_name == "hasToken")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_EQUALS;
            out.bloom_filter = std::make_unique<BloomFilter>(params);
            stringToBloomFilter(const_value.get<String>(), token_extractor, *out.bloom_filter);
            return true;
        }
        else if (func_name == "startsWith")
        {
            out.key_column = key_column_num;
            return createFunctionEqualsCondition(out, const_value, params, token_extractor);
        }
        else if (func_name == "endsWith")
        {
            out.key_column = key_column_num;
            return createFunctionEqualsCondition(out, const_value, params, token_extractor);
        }
        else if (func_name == "multiSearchAny")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_MULTI_SEARCH;

            /// 2d vector is not needed here but is used because already exists for FUNCTION_IN
            std::vector<std::vector<BloomFilter>> bloom_filters;
            bloom_filters.emplace_back();
            for (const auto & element : const_value.get<Array>())
            {
                if (element.getType() != Field::Types::String)
                    return false;

                bloom_filters.back().emplace_back(params);
                stringToBloomFilter(element.get<String>(), token_extractor, bloom_filters.back().back());
            }
            out.set_bloom_filters = std::move(bloom_filters);
            return true;
        }
        else if (func_name == "notIn")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_NOT_IN;
            return true;
        }
        else if (func_name == "in")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_IN;
            return true;
        }

        return false;
    }
    else if (KeyCondition::getConstant(node, block_with_constants, const_value, const_type))
    {
        /// Check constant like in KeyCondition
        if (const_value.getType() == Field::Types::UInt64
            || const_value.getType() == Field::Types::Int64
            || const_value.getType() == Field::Types::Float64)
        {
            /// Zero in all types is represented in memory the same way as in UInt64.
            out.function = const_value.get<UInt64>()
                           ? RPNElement::ALWAYS_TRUE
                           : RPNElement::ALWAYS_FALSE;

            return true;
        }
    }

    return false;
}

bool MergeTreeConditionFullText::tryPrepareSetBloomFilter(
    const ASTs & args,
    RPNElement & out)
{
    const ASTPtr & left_arg = args[0];
    const ASTPtr & right_arg = args[1];

    std::vector<KeyTuplePositionMapping> key_tuple_mapping;
    DataTypes data_types;

    const auto * left_arg_tuple = typeid_cast<const ASTFunction *>(left_arg.get());
    if (left_arg_tuple && left_arg_tuple->name == "tuple")
    {
        const auto & tuple_elements = left_arg_tuple->arguments->children;
        for (size_t i = 0; i < tuple_elements.size(); ++i)
        {
            size_t key = 0;
            if (getKey(tuple_elements[i], key))
            {
                key_tuple_mapping.emplace_back(i, key);
                data_types.push_back(index_data_types[key]);
            }
        }
    }
    else
    {
        size_t key = 0;
        if (getKey(left_arg, key))
        {
            key_tuple_mapping.emplace_back(0, key);
            data_types.push_back(index_data_types[key]);
        }
    }

    if (key_tuple_mapping.empty())
        return false;

    PreparedSetKey set_key;
    if (typeid_cast<const ASTSubquery *>(right_arg.get()) || typeid_cast<const ASTIdentifier *>(right_arg.get()))
        set_key = PreparedSetKey::forSubquery(*right_arg);
    else
        set_key = PreparedSetKey::forLiteral(*right_arg, data_types);

    auto set_it = prepared_sets.find(set_key);
    if (set_it == prepared_sets.end())
        return false;

    const SetPtr & prepared_set = set_it->second;
    if (!prepared_set->hasExplicitSetElements())
        return false;

    for (const auto & data_type : prepared_set->getDataTypes())
        if (data_type->getTypeId() != TypeIndex::String && data_type->getTypeId() != TypeIndex::FixedString)
            return false;

    std::vector<std::vector<BloomFilter>> bloom_filters;
    std::vector<size_t> key_position;

    Columns columns = prepared_set->getSetElements();
    for (const auto & elem : key_tuple_mapping)
    {
        bloom_filters.emplace_back();
        key_position.push_back(elem.key_index);

        size_t tuple_idx = elem.tuple_index;
        const auto & column = columns[tuple_idx];
        for (size_t row = 0; row < prepared_set->getTotalRowCount(); ++row)
        {
            bloom_filters.back().emplace_back(params);
            auto ref = column->getDataAt(row);
            columnToBloomFilter(ref.data, ref.size, token_extractor, bloom_filters.back().back());
        }
    }

    out.set_key_position = std::move(key_position);
    out.set_bloom_filters = std::move(bloom_filters);

    return true;
}

MergeTreeIndexGranulePtr MergeTreeIndexFullText::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleFullText>(index.name, index.column_names.size(), params);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexFullText::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorFullText>(index.column_names, index.name, params, token_extractor.get());
}

MergeTreeIndexConditionPtr MergeTreeIndexFullText::createIndexCondition(
        const SelectQueryInfo & query, ContextPtr context) const
{
    return std::make_shared<MergeTreeConditionFullText>(query, context, index.sample_block, params, token_extractor.get());
};

bool MergeTreeIndexFullText::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    return std::find(std::cbegin(index.column_names), std::cend(index.column_names), node->getColumnName()) != std::cend(index.column_names);
}


bool NgramTokenExtractor::nextInField(const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len) const
{
    *token_start = *pos;
    *token_len = 0;
    size_t code_points = 0;
    for (; code_points < n && *token_start + *token_len < len; ++code_points)
    {
        size_t sz = UTF8::seqLength(static_cast<UInt8>(data[*token_start + *token_len]));
        *token_len += sz;
    }
    *pos += UTF8::seqLength(static_cast<UInt8>(data[*pos]));
    return code_points == n;
}

bool NgramTokenExtractor::nextLike(const String & str, size_t * pos, String & token) const
{
    token.clear();

    size_t code_points = 0;
    bool escaped = false;
    for (size_t i = *pos; i < str.size();)
    {
        if (escaped && (str[i] == '%' || str[i] == '_' || str[i] == '\\'))
        {
            token += str[i];
            ++code_points;
            escaped = false;
            ++i;
        }
        else if (!escaped && (str[i] == '%' || str[i] == '_'))
        {
            /// This token is too small, go to the next.
            token.clear();
            code_points = 0;
            escaped = false;
            *pos = ++i;
        }
        else if (!escaped && str[i] == '\\')
        {
            escaped = true;
            ++i;
        }
        else
        {
            const size_t sz = UTF8::seqLength(static_cast<UInt8>(str[i]));
            for (size_t j = 0; j < sz; ++j)
                token += str[i + j];
            i += sz;
            ++code_points;
            escaped = false;
        }

        if (code_points == n)
        {
            *pos += UTF8::seqLength(static_cast<UInt8>(str[*pos]));
            return true;
        }
    }

    return false;
}


bool SplitTokenExtractor::nextInField(const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len) const
{
    *token_start = *pos;
    *token_len = 0;

    while (*pos < len)
    {
        if (isASCII(data[*pos]) && !isAlphaNumericASCII(data[*pos]))
        {
            /// Finish current token if any
            if (*token_len > 0)
                return true;
            *token_start = ++*pos;
        }
        else
        {
            /// Note that UTF-8 sequence is completely consisted of non-ASCII bytes.
            ++*pos;
            ++*token_len;
        }
    }

    return *token_len > 0;
}

bool SplitTokenExtractor::nextInColumn(const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len) const
{
    *token_start = *pos;
    *token_len = 0;

    while (*pos < len)
    {
#if defined(__SSE2__) && !defined(MEMORY_SANITIZER) /// We read uninitialized bytes and decide on the calculated mask
        // NOTE: we assume that `data` string is padded from the right with 15 bytes.
        const __m128i haystack = _mm_loadu_si128(reinterpret_cast<const __m128i *>(data + *pos));
        const size_t haystack_length = 16;

#if defined(__SSE4_2__)
        // With the help of https://www.strchr.com/strcmp_and_strlen_using_sse_4.2
        const auto alnum_chars_ranges = _mm_set_epi8(0, 0, 0, 0, 0, 0, 0, 0,
                '\xFF', '\x80', 'z', 'a', 'Z', 'A', '9', '0');
        // Every bit represents if `haystack` character is in the ranges (1) or not (0)
        const int result_bitmask = _mm_cvtsi128_si32(_mm_cmpestrm(alnum_chars_ranges, 8, haystack, haystack_length, _SIDD_CMP_RANGES));
#else
        // NOTE: -1 and +1 required since SSE2 has no `>=` and `<=` instructions on packed 8-bit integers (epi8).
        const auto number_begin =      _mm_set1_epi8('0' - 1);
        const auto number_end =        _mm_set1_epi8('9' + 1);
        const auto alpha_lower_begin = _mm_set1_epi8('a' - 1);
        const auto alpha_lower_end =   _mm_set1_epi8('z' + 1);
        const auto alpha_upper_begin = _mm_set1_epi8('A' - 1);
        const auto alpha_upper_end =   _mm_set1_epi8('Z' + 1);
        const auto zero =              _mm_set1_epi8(0);

        // every bit represents if `haystack` character `c` satisfies condition:
        // (c < 0) || (c > '0' - 1 && c < '9' + 1) || (c > 'a' - 1 && c < 'z' + 1) || (c > 'A' - 1 && c < 'Z' + 1)
        // < 0 since _mm_cmplt_epi8 threats chars as SIGNED, and so all chars > 0x80 are negative.
        const int result_bitmask = _mm_movemask_epi8(_mm_or_si128(_mm_or_si128(_mm_or_si128(
                _mm_cmplt_epi8(haystack, zero),
                _mm_and_si128(_mm_cmpgt_epi8(haystack, number_begin),      _mm_cmplt_epi8(haystack, number_end))),
                _mm_and_si128(_mm_cmpgt_epi8(haystack, alpha_lower_begin), _mm_cmplt_epi8(haystack, alpha_lower_end))),
                _mm_and_si128(_mm_cmpgt_epi8(haystack, alpha_upper_begin), _mm_cmplt_epi8(haystack, alpha_upper_end))));
#endif
        if (result_bitmask == 0)
        {
            if (*token_len != 0)
                // end of token started on previous haystack
                return true;

            *pos += haystack_length;
            continue;
        }

        const auto token_start_pos_in_current_haystack = getTrailingZeroBitsUnsafe(result_bitmask);
        if (*token_len == 0)
            // new token
            *token_start = *pos + token_start_pos_in_current_haystack;
        else if (token_start_pos_in_current_haystack != 0)
            // end of token starting in one of previous haystacks
            return true;

        const auto token_bytes_in_current_haystack = getTrailingZeroBitsUnsafe(~(result_bitmask >> token_start_pos_in_current_haystack));
        *token_len += token_bytes_in_current_haystack;

        *pos += token_start_pos_in_current_haystack + token_bytes_in_current_haystack;
        if (token_start_pos_in_current_haystack + token_bytes_in_current_haystack == haystack_length)
            // check if there are leftovers in next `haystack`
            continue;

        break;
#else
        if (isASCII(data[*pos]) && !isAlphaNumericASCII(data[*pos]))
        {
            /// Finish current token if any
            if (*token_len > 0)
                return true;
            *token_start = ++*pos;
        }
        else
        {
            /// Note that UTF-8 sequence is completely consisted of non-ASCII bytes.
            ++*pos;
            ++*token_len;
        }
#endif
    }

#if defined(__SSE2__) && !defined(MEMORY_SANITIZER)
    // Could happen only if string is not padded with zeros, and we accidentally hopped over the end of data.
    if (*token_start > len)
        return false;
    *token_len = std::min(len - *token_start, *token_len);
#endif

    return *token_len > 0;
}

bool SplitTokenExtractor::nextLike(const String & str, size_t * pos, String & token) const
{
    token.clear();
    bool bad_token = false; // % or _ before token
    bool escaped = false;
    while (*pos < str.size())
    {
        if (!escaped && (str[*pos] == '%' || str[*pos] == '_'))
        {
            token.clear();
            bad_token = true;
            ++*pos;
        }
        else if (!escaped && str[*pos] == '\\')
        {
            escaped = true;
            ++*pos;
        }
        else if (isASCII(str[*pos]) && !isAlphaNumericASCII(str[*pos]))
        {
            if (!bad_token && !token.empty())
                return true;

            token.clear();
            bad_token = false;
            escaped = false;
            ++*pos;
        }
        else
        {
            const size_t sz = UTF8::seqLength(static_cast<UInt8>(str[*pos]));
            for (size_t j = 0; j < sz; ++j)
            {
                token += str[*pos];
                ++*pos;
            }
            escaped = false;
        }
    }

    return !bad_token && !token.empty();
}


MergeTreeIndexPtr bloomFilterIndexCreator(
    const IndexDescription & index)
{
    if (index.type == NgramTokenExtractor::getName())
    {
        size_t n = index.arguments[0].get<size_t>();
        BloomFilterParameters params(
            index.arguments[1].get<size_t>(),
            index.arguments[2].get<size_t>(),
            index.arguments[3].get<size_t>());

        auto tokenizer = std::make_unique<NgramTokenExtractor>(n);

        return std::make_shared<MergeTreeIndexFullText>(index, params, std::move(tokenizer));
    }
    else if (index.type == SplitTokenExtractor::getName())
    {
        BloomFilterParameters params(
            index.arguments[0].get<size_t>(),
            index.arguments[1].get<size_t>(),
            index.arguments[2].get<size_t>());

        auto tokenizer = std::make_unique<SplitTokenExtractor>();

        return std::make_shared<MergeTreeIndexFullText>(index, params, std::move(tokenizer));
    }
    else
    {
        throw Exception("Unknown index type: " + backQuote(index.name), ErrorCodes::LOGICAL_ERROR);
    }
}

void bloomFilterIndexValidator(const IndexDescription & index, bool /*attach*/)
{
    for (const auto & data_type : index.data_types)
    {
        if (data_type->getTypeId() != TypeIndex::String && data_type->getTypeId() != TypeIndex::FixedString)
            throw Exception("Bloom filter index can be used only with `String` or `FixedString` column.", ErrorCodes::INCORRECT_QUERY);
    }

    if (index.type == NgramTokenExtractor::getName())
    {
        if (index.arguments.size() != 4)
            throw Exception("`ngrambf` index must have exactly 4 arguments.", ErrorCodes::INCORRECT_QUERY);
    }
    else if (index.type == SplitTokenExtractor::getName())
    {
        if (index.arguments.size() != 3)
            throw Exception("`tokenbf` index must have exactly 3 arguments.", ErrorCodes::INCORRECT_QUERY);
    }
    else
    {
        throw Exception("Unknown index type: " + backQuote(index.name), ErrorCodes::LOGICAL_ERROR);
    }

    assert(index.arguments.size() >= 3);

    for (const auto & arg : index.arguments)
        if (arg.getType() != Field::Types::UInt64)
            throw Exception("All parameters to *bf_v1 index must be unsigned integers", ErrorCodes::BAD_ARGUMENTS);

    /// Just validate
    BloomFilterParameters params(
        index.arguments[0].get<size_t>(),
        index.arguments[1].get<size_t>(),
        index.arguments[2].get<size_t>());
}

}
