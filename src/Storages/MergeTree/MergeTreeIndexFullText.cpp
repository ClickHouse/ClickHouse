#include <Storages/MergeTree/MergeTreeIndexFullText.h>

#include <Common/StringUtils/StringUtils.h>
#include <Common/UTF8Helpers.h>
#include <DataTypes/DataTypesNumber.h>
#include <IO/WriteHelpers.h>
#include <IO/ReadHelpers.h>
#include <Interpreters/ExpressionActions.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/SyntaxAnalyzer.h>
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
}


/// Adds all tokens from string to bloom filter.
static void stringToBloomFilter(
    const char * data, size_t size, const std::unique_ptr<ITokenExtractor> & token_extractor, BloomFilter & bloom_filter)
{
    size_t cur = 0;
    size_t token_start = 0;
    size_t token_len = 0;
    while (cur < size && token_extractor->next(data, size, &cur, &token_start, &token_len))
        bloom_filter.add(data + token_start, token_len);
}

/// Adds all tokens from like pattern string to bloom filter. (Because like pattern can contain `\%` and `\_`.)
static void likeStringToBloomFilter(
    const String & data, const std::unique_ptr<ITokenExtractor> & token_extractor, BloomFilter & bloom_filter)
{
    size_t cur = 0;
    String token;
    while (cur < data.size() && token_extractor->nextLike(data, &cur, token))
        bloom_filter.add(token.c_str(), token.size());
}
/// Unified condition for equals, startsWith and endsWith
bool MergeTreeConditionFullText::createFunctionEqualsCondition(RPNElement & out, const Field & value, const MergeTreeIndexFullText & idx)
{
    out.function = RPNElement::FUNCTION_EQUALS;
    out.bloom_filter = std::make_unique<BloomFilter>(
            idx.bloom_filter_size, idx.bloom_filter_hashes, idx.seed);

    const auto & str = value.get<String>();
    stringToBloomFilter(str.c_str(), str.size(), idx.token_extractor_func, *out.bloom_filter);
    return true;
}

MergeTreeIndexGranuleFullText::MergeTreeIndexGranuleFullText(const MergeTreeIndexFullText & index_)
    : index(index_)
    , bloom_filters(
            index.columns.size(), BloomFilter(index.bloom_filter_size, index.bloom_filter_hashes, index.seed))
    , has_elems(false) {}

void MergeTreeIndexGranuleFullText::serializeBinary(WriteBuffer & ostr) const
{
    if (empty())
        throw Exception("Attempt to write empty minmax index " + backQuote(index.name), ErrorCodes::LOGICAL_ERROR);

    for (const auto & bloom_filter : bloom_filters)
        ostr.write(reinterpret_cast<const char *>(bloom_filter.getFilter().data()), index.bloom_filter_size);
}

void MergeTreeIndexGranuleFullText::deserializeBinary(ReadBuffer & istr)
{
    for (auto & bloom_filter : bloom_filters)
    {
        istr.read(reinterpret_cast<char *>(bloom_filter.getFilter().data()), index.bloom_filter_size);
    }
    has_elems = true;
}


MergeTreeIndexAggregatorFullText::MergeTreeIndexAggregatorFullText(const MergeTreeIndexFullText & index_)
    : index(index_), granule(std::make_shared<MergeTreeIndexGranuleFullText>(index)) {}

MergeTreeIndexGranulePtr MergeTreeIndexAggregatorFullText::getGranuleAndReset()
{
    auto new_granule = std::make_shared<MergeTreeIndexGranuleFullText>(index);
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

    for (size_t col = 0; col < index.columns.size(); ++col)
    {
        const auto & column = block.getByName(index.columns[col]).column;
        for (size_t i = 0; i < rows_read; ++i)
        {
            auto ref = column->getDataAt(*pos + i);
            stringToBloomFilter(ref.data, ref.size, index.token_extractor_func, granule->bloom_filters[col]);
        }
    }
    granule->has_elems = true;
    *pos += rows_read;
}


MergeTreeConditionFullText::MergeTreeConditionFullText(
    const SelectQueryInfo & query_info,
    const Context & context,
    const MergeTreeIndexFullText & index_) : index(index_), prepared_sets(query_info.sets)
{
    rpn = std::move(
            RPNBuilder<RPNElement>(
                    query_info, context,
                    [this] (const ASTPtr & node, const Context & /* context */, Block & block_with_constants, RPNElement & out) -> bool
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
    auto it = std::find(index.columns.begin(), index.columns.end(), node->getColumnName());
    if (it == index.columns.end())
        return false;

    key_column_num = static_cast<size_t>(it - index.columns.begin());
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

        if (key_arg_pos == 1 && (func_name != "equals" || func_name != "notEquals"))
            return false;
        else if (!index.token_extractor_func->supportLike() && (func_name == "like" || func_name == "notLike"))
            return false;

        if (func_name == "notEquals")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_NOT_EQUALS;
            out.bloom_filter = std::make_unique<BloomFilter>(
                    index.bloom_filter_size, index.bloom_filter_hashes, index.seed);

            const auto & str = const_value.get<String>();
            stringToBloomFilter(str.c_str(), str.size(), index.token_extractor_func, *out.bloom_filter);
            return true;
        }
        else if (func_name == "equals")
        {
            out.key_column = key_column_num;
            return createFunctionEqualsCondition(out, const_value, index);
        }
        else if (func_name == "like")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_EQUALS;
            out.bloom_filter = std::make_unique<BloomFilter>(
                    index.bloom_filter_size, index.bloom_filter_hashes, index.seed);

            const auto & str = const_value.get<String>();
            likeStringToBloomFilter(str, index.token_extractor_func, *out.bloom_filter);
            return true;
        }
        else if (func_name == "notLike")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_NOT_EQUALS;
            out.bloom_filter = std::make_unique<BloomFilter>(
                    index.bloom_filter_size, index.bloom_filter_hashes, index.seed);

            const auto & str = const_value.get<String>();
            likeStringToBloomFilter(str, index.token_extractor_func, *out.bloom_filter);
            return true;
        }
        else if (func_name == "hasToken")
        {
            out.key_column = key_column_num;
            out.function = RPNElement::FUNCTION_EQUALS;
            out.bloom_filter = std::make_unique<BloomFilter>(
                    index.bloom_filter_size, index.bloom_filter_hashes, index.seed);

            const auto & str = const_value.get<String>();
            stringToBloomFilter(str.c_str(), str.size(), index.token_extractor_func, *out.bloom_filter);
            return true;
        }
        else if (func_name == "startsWith")
        {
            out.key_column = key_column_num;
            return createFunctionEqualsCondition(out, const_value, index);
        }
        else if (func_name == "endsWith")
        {
            out.key_column = key_column_num;
            return createFunctionEqualsCondition(out, const_value, index);
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

                bloom_filters.back().emplace_back(index.bloom_filter_size, index.bloom_filter_hashes, index.seed);
                const auto & str = element.get<String>();
                stringToBloomFilter(str.c_str(), str.size(), index.token_extractor_func, bloom_filters.back().back());
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
                data_types.push_back(index.data_types[key]);
            }
        }
    }
    else
    {
        size_t key = 0;
        if (getKey(left_arg, key))
        {
            key_tuple_mapping.emplace_back(0, key);
            data_types.push_back(index.data_types[key]);
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
            bloom_filters.back().emplace_back(index.bloom_filter_size, index.bloom_filter_hashes, index.seed);
            auto ref = column->getDataAt(row);
            stringToBloomFilter(ref.data, ref.size, index.token_extractor_func, bloom_filters.back().back());
        }
    }

    out.set_key_position = std::move(key_position);
    out.set_bloom_filters = std::move(bloom_filters);

    return true;
}

MergeTreeIndexGranulePtr MergeTreeIndexFullText::createIndexGranule() const
{
    return std::make_shared<MergeTreeIndexGranuleFullText>(*this);
}

MergeTreeIndexAggregatorPtr MergeTreeIndexFullText::createIndexAggregator() const
{
    return std::make_shared<MergeTreeIndexAggregatorFullText>(*this);
}

MergeTreeIndexConditionPtr MergeTreeIndexFullText::createIndexCondition(
        const SelectQueryInfo & query, const Context & context) const
{
    return std::make_shared<MergeTreeConditionFullText>(query, context, *this);
};

bool MergeTreeIndexFullText::mayBenefitFromIndexForIn(const ASTPtr & node) const
{
    return std::find(std::cbegin(columns), std::cend(columns), node->getColumnName()) != std::cend(columns);
}


bool NgramTokenExtractor::next(const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len) const
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

bool SplitTokenExtractor::next(const char * data, size_t len, size_t * pos, size_t * token_start, size_t * token_len) const
{
    *token_start = *pos;
    *token_len = 0;

    while (*pos < len)
    {
#if defined(__SSE2__) && !defined(MEMORY_SANITIZER) /// We read uninitialized bytes and decide on the calcualted mask
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
        const auto zero  =             _mm_set1_epi8(0);

        // every bit represents if `haystack` character `c` statisfies condition:
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
    // Could happen only if string is not padded with zeroes, and we accidentally hopped over end of data.
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


std::unique_ptr<IMergeTreeIndex> bloomFilterIndexCreator(
    const NamesAndTypesList & new_columns,
    std::shared_ptr<ASTIndexDeclaration> node,
    const Context & context,
    bool /*attach*/)
{
    if (node->name.empty())
        throw Exception("Index must have unique name", ErrorCodes::INCORRECT_QUERY);

    ASTPtr expr_list = MergeTreeData::extractKeyExpressionList(node->expr->clone());

    auto syntax = SyntaxAnalyzer(context).analyze(expr_list, new_columns);
    auto index_expr = ExpressionAnalyzer(expr_list, syntax, context).getActions(false);

    auto sample = ExpressionAnalyzer(expr_list, syntax, context)
            .getActions(true)->getSampleBlock();

    Names columns;
    DataTypes data_types;

    for (size_t i = 0; i < expr_list->children.size(); ++i)
    {
        const auto & column = sample.getByPosition(i);

        columns.emplace_back(column.name);
        data_types.emplace_back(column.type);

        if (data_types.back()->getTypeId() != TypeIndex::String
            && data_types.back()->getTypeId() != TypeIndex::FixedString)
            throw Exception("Bloom filter index can be used only with `String` or `FixedString` column.", ErrorCodes::INCORRECT_QUERY);
    }

    boost::algorithm::to_lower(node->type->name);
    if (node->type->name == NgramTokenExtractor::getName())
    {
        if (!node->type->arguments || node->type->arguments->children.size() != 4)
            throw Exception("`ngrambf` index must have exactly 4 arguments.", ErrorCodes::INCORRECT_QUERY);

        size_t n = typeid_cast<const ASTLiteral &>(
                *node->type->arguments->children[0]).value.safeGet<size_t>();
        size_t bloom_filter_size = typeid_cast<const ASTLiteral &>(
                *node->type->arguments->children[1]).value.safeGet<size_t>();
        size_t bloom_filter_hashes = typeid_cast<const ASTLiteral &>(
                *node->type->arguments->children[2]).value.safeGet<size_t>();
        size_t seed = typeid_cast<const ASTLiteral &>(
                *node->type->arguments->children[3]).value.safeGet<size_t>();

        auto tokenizer = std::make_unique<NgramTokenExtractor>(n);

        return std::make_unique<MergeTreeIndexFullText>(
                node->name, std::move(index_expr), columns, data_types, sample, node->granularity,
                bloom_filter_size, bloom_filter_hashes, seed, std::move(tokenizer));
    }
    else if (node->type->name == SplitTokenExtractor::getName())
    {
        if (!node->type->arguments || node->type->arguments->children.size() != 3)
            throw Exception("`tokenbf` index must have exactly 3 arguments.", ErrorCodes::INCORRECT_QUERY);

        size_t bloom_filter_size = typeid_cast<const ASTLiteral &>(
                *node->type->arguments->children[0]).value.safeGet<size_t>();
        size_t bloom_filter_hashes = typeid_cast<const ASTLiteral &>(
                *node->type->arguments->children[1]).value.safeGet<size_t>();
        size_t seed = typeid_cast<const ASTLiteral &>(
                *node->type->arguments->children[2]).value.safeGet<size_t>();

        auto tokenizer = std::make_unique<SplitTokenExtractor>();

        return std::make_unique<MergeTreeIndexFullText>(
                node->name, std::move(index_expr), columns, data_types, sample, node->granularity,
                bloom_filter_size, bloom_filter_hashes, seed, std::move(tokenizer));
    }
    else
    {
        throw Exception("Unknown index type: " + backQuote(node->name), ErrorCodes::LOGICAL_ERROR);
    }
}

}
