#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnBLOB.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnLazy.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnQBit.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/ColumnsScatter.h>
#include <Columns/IColumnDummy.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Common/Exception.h>
#include <Common/PODArray.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/typeid_cast.h>

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <pcg_random.hpp>

#include <algorithm>
#include <array>
#include <cmath>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <numeric>
#include <span>
#include <string_view>
#include <unordered_set>
#include <utility>
#include <vector>

using namespace DB;

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NOT_IMPLEMENTED;
    extern const int SIZES_OF_COLUMNS_DOESNT_MATCH;
}

namespace
{

constexpr UInt64 selector_seed = 20261107;

template <typename Id>
struct SelectorCase
{
    String name;
    size_t pieces = 0;
    PaddedPODArray<Id> selector;
    /// At most 1000 rows in at most 1024 pieces, plus 1000 rows with uniform ids in 65536 pieces.
    bool in_capped_set = false;
    /// At most 17 rows in at most 17 pieces, plus 1000 rows with uniform or skewed ids in 3 or 256 pieces.
    bool in_short_set = false;
};

/// Every row count with every piece count and every distribution, plus the cases without rows and the cases
/// with every row in its own piece. With 16-bit ids, the cases that need more than 65536 pieces are left out.
template <typename Id>
std::vector<SelectorCase<Id>> makeSelectorCases()
{
    constexpr bool small_ids = std::is_same_v<Id, UInt16>;
    constexpr size_t max_pieces = small_ids ? 65536 : std::numeric_limits<size_t>::max();
    constexpr std::array<size_t, 6> row_counts = {1, 15, 16, 17, 1000, 70000};
    constexpr std::array<size_t, 9> piece_counts = {1, 2, 3, 16, 255, 256, 1024, 65536, 65537};

    pcg64 rng(selector_seed);

    std::vector<SelectorCase<Id>> cases;
    const auto add = [&](size_t rows, size_t pieces, std::string_view distribution, const std::vector<UInt64> & ids)
    {
        SelectorCase<Id> selector_case;
        selector_case.name = fmt::format("rows={} pieces={} {}", rows, pieces, distribution);
        selector_case.pieces = pieces;
        selector_case.selector.reserve_exact(ids.size());
        for (const UInt64 id : ids)
            selector_case.selector.push_back(static_cast<Id>(id));
        selector_case.in_capped_set = (rows <= 1000 && pieces <= 1024) || (rows == 1000 && pieces == 65536 && distribution == "uniform");
        selector_case.in_short_set = (rows <= 17 && pieces <= 17)
            || (rows == 1000 && (pieces == 3 || pieces == 256) && (distribution == "uniform" || distribution == "skewed"));
        cases.push_back(std::move(selector_case));
    };

    for (const size_t pieces : {size_t{0}, size_t{1}, size_t{5}})
        add(0, pieces, "no rows", {});

    for (const size_t rows : row_counts)
    {
        for (const size_t pieces : piece_counts)
        {
            if (pieces > max_pieces)
                continue;

            /// With 65536 or more pieces, some rows go to the last piece: id 65535 for 16-bit ids.
            const auto route_to_last_piece = [pieces](std::vector<UInt64> & ids, bool keep_sorted)
            {
                if (pieces < 65536)
                    return;
                if (keep_sorted)
                {
                    for (size_t row = ids.size() - ids.size() / 16 - 1; row < ids.size(); ++row)
                        ids[row] = pieces - 1;
                    return;
                }
                for (size_t row = 63; row < ids.size(); row += 64)
                    ids[row] = pieces - 1;
                ids.back() = pieces - 1;
            };

            add(rows, pieces, "all in piece 0", std::vector<UInt64>(rows, 0));
            if (pieces > 1)
                add(rows, pieces, "all in the middle piece", std::vector<UInt64>(rows, pieces / 2));

            std::vector<UInt64> ids(rows);
            for (auto & id : ids)
                id = rng() % pieces;
            route_to_last_piece(ids, false);
            add(rows, pieces, "uniform", ids);

            for (auto & id : ids)
                id = rng() % 10 == 0 ? rng() % pieces : 0;
            route_to_last_piece(ids, false);
            add(rows, pieces, "skewed", ids);

            for (size_t row = 0; row < rows; ++row)
                ids[row] = row * pieces / rows;
            route_to_last_piece(ids, true);
            add(rows, pieces, "sorted runs", ids);
        }
    }

    for (const size_t rows : row_counts)
    {
        if (rows > max_pieces)
            continue;
        std::vector<UInt64> ids(rows);
        std::iota(ids.begin(), ids.end(), UInt64{0});
        add(rows, rows, "every row its own piece", ids);
    }

    return cases;
}

template <typename Id>
const std::vector<SelectorCase<Id>> & selectorCases()
{
    static const std::vector<SelectorCase<Id>> cases = makeSelectorCases<Id>();
    return cases;
}

/// The rows of each piece, in row order.
class PieceRows
{
public:
    template <typename Id>
    PieceRows(const PaddedPODArray<Id> & selector, size_t num_pieces)
        : begins(num_pieces + 1, 0)
        , rows(selector.size())
    {
        for (const auto id : selector)
            ++begins[id + 1];
        for (size_t piece = 0; piece < num_pieces; ++piece)
            begins[piece + 1] += begins[piece];

        std::vector<size_t> next(begins.begin(), begins.end() - 1);
        for (size_t row = 0; row < selector.size(); ++row)
            rows[next[selector[row]]++] = static_cast<UInt32>(row);
    }

    size_t size() const { return begins.size() - 1; }
    std::span<const UInt32> of(size_t piece) const { return {rows.data() + begins[piece], begins[piece + 1] - begins[piece]}; }

private:
    std::vector<size_t> begins;
    std::vector<UInt32> rows;
};

testing::AssertionResult sameColumn(const IColumn & expected, const IColumn & actual, bool compare_names);

testing::AssertionResult samePart(const char * part, const IColumn & expected, const IColumn & actual)
{
    auto result = sameColumn(expected, actual, false);
    if (!result)
        result << " in " << part;
    return result;
}

template <typename Container>
std::string_view bytesOf(const Container & data)
{
    return {reinterpret_cast<const char *>(data.data()), data.size() * sizeof(data[0])};
}

testing::AssertionResult sameBytes(std::string_view expected, std::string_view actual)
{
    if (expected == actual)
        return testing::AssertionSuccess();

    size_t offset = 0;
    while (offset < expected.size() && offset < actual.size() && expected[offset] == actual[offset])
        ++offset;
    return testing::AssertionFailure() << "the bytes differ at offset " << offset << " (" << actual.size() << " bytes, expected "
                                       << expected.size() << ")";
}

/// Compares byte for byte, part by part, and fails for a column layout it does not know.
/// `compare_names` also compares `getName` and `dumpStructure`, which allocate strings.
testing::AssertionResult sameColumn(const IColumn & expected, const IColumn & actual, bool compare_names)
{
    if (std::strcmp(expected.getFamilyName(), actual.getFamilyName()) != 0)
        return testing::AssertionFailure() << "family " << actual.getFamilyName() << ", expected " << expected.getFamilyName();
    if (expected.size() != actual.size())
        return testing::AssertionFailure() << actual.getFamilyName() << " has " << actual.size() << " rows, expected " << expected.size();
    if (expected.isConst() != actual.isConst() || expected.isSparse() != actual.isSparse()
        || expected.isReplicated() != actual.isReplicated())
        return testing::AssertionFailure() << "representation " << actual.dumpStructure() << ", expected " << expected.dumpStructure();
    if (compare_names && expected.getName() != actual.getName())
        return testing::AssertionFailure() << "name " << actual.getName() << ", expected " << expected.getName();
    if (compare_names && expected.dumpStructure() != actual.dumpStructure())
        return testing::AssertionFailure() << "structure " << actual.dumpStructure() << ", expected " << expected.dumpStructure();

    if (const auto * expected_const = typeid_cast<const ColumnConst *>(&expected))
        return samePart("Const data", expected_const->getDataColumn(), assert_cast<const ColumnConst &>(actual).getDataColumn());

    if (const auto * expected_sparse = typeid_cast<const ColumnSparse *>(&expected))
    {
        const auto & actual_sparse = assert_cast<const ColumnSparse &>(actual);
        if (auto result = samePart("Sparse values", expected_sparse->getValuesColumn(), actual_sparse.getValuesColumn()); !result)
            return result;
        return samePart("Sparse offsets", expected_sparse->getOffsetsColumn(), actual_sparse.getOffsetsColumn());
    }

    if (const auto * expected_replicated = typeid_cast<const ColumnReplicated *>(&expected))
    {
        const auto & actual_replicated = assert_cast<const ColumnReplicated &>(actual);
        if (auto result
            = samePart("Replicated nested column", *expected_replicated->getNestedColumn(), *actual_replicated.getNestedColumn());
            !result)
            return result;
        return samePart("Replicated indexes", *expected_replicated->getIndexesColumn(), *actual_replicated.getIndexesColumn());
    }

    if (const auto * expected_low_cardinality = typeid_cast<const ColumnLowCardinality *>(&expected))
    {
        const auto & actual_low_cardinality = assert_cast<const ColumnLowCardinality &>(actual);
        if (expected_low_cardinality->getSizeOfIndexType() != actual_low_cardinality.getSizeOfIndexType())
            return testing::AssertionFailure() << "index width " << actual_low_cardinality.getSizeOfIndexType() << ", expected "
                                               << expected_low_cardinality->getSizeOfIndexType();
        if (expected_low_cardinality->isSharedDictionary() != actual_low_cardinality.isSharedDictionary())
            return testing::AssertionFailure() << "isSharedDictionary is " << actual_low_cardinality.isSharedDictionary();

        const auto & expected_dictionary = expected_low_cardinality->getDictionary();
        const auto & actual_dictionary = actual_low_cardinality.getDictionary();
        if (expected_dictionary.nestedColumnIsNullable() != actual_dictionary.nestedColumnIsNullable())
            return testing::AssertionFailure() << "the dictionary differs in nullability";
        if (auto result = samePart(
                "LowCardinality dictionary",
                *expected_dictionary.getNestedNotNullableColumn(),
                *actual_dictionary.getNestedNotNullableColumn());
            !result)
            return result;
        return samePart("LowCardinality indexes", expected_low_cardinality->getIndexes(), actual_low_cardinality.getIndexes());
    }

    if (const auto * expected_nullable = typeid_cast<const ColumnNullable *>(&expected))
    {
        const auto & actual_nullable = assert_cast<const ColumnNullable &>(actual);
        if (auto result = samePart("null map", expected_nullable->getNullMapColumn(), actual_nullable.getNullMapColumn()); !result)
            return result;
        return samePart("Nullable nested column", expected_nullable->getNestedColumn(), actual_nullable.getNestedColumn());
    }

    if (const auto * expected_array = typeid_cast<const ColumnArray *>(&expected))
    {
        const auto & actual_array = assert_cast<const ColumnArray &>(actual);
        if (auto result = samePart("Array offsets", expected_array->getOffsetsColumn(), actual_array.getOffsetsColumn()); !result)
            return result;
        return samePart("Array data", expected_array->getData(), actual_array.getData());
    }

    if (const auto * expected_tuple = typeid_cast<const ColumnTuple *>(&expected))
    {
        const auto & actual_tuple = assert_cast<const ColumnTuple &>(actual);
        if (expected_tuple->tupleSize() != actual_tuple.tupleSize())
            return testing::AssertionFailure() << actual_tuple.tupleSize() << " Tuple elements, expected " << expected_tuple->tupleSize();
        for (size_t element = 0; element < expected_tuple->tupleSize(); ++element)
        {
            if (auto result = samePart("Tuple element", expected_tuple->getColumn(element), actual_tuple.getColumn(element)); !result)
                return result << " " << element;
        }
        return testing::AssertionSuccess();
    }

    if (const auto * expected_map = typeid_cast<const ColumnMap *>(&expected))
        return samePart("Map nested column", expected_map->getNestedColumn(), assert_cast<const ColumnMap &>(actual).getNestedColumn());

    if (const auto * expected_string = typeid_cast<const ColumnString *>(&expected))
    {
        const auto & actual_string = assert_cast<const ColumnString &>(actual);
        if (auto result = sameBytes(bytesOf(expected_string->getOffsets()), bytesOf(actual_string.getOffsets())); !result)
            return result << " in String offsets";
        if (auto result = sameBytes(bytesOf(expected_string->getChars()), bytesOf(actual_string.getChars())); !result)
            return result << " in String chars";
        return testing::AssertionSuccess();
    }

    /// The offsets of a `Variant` are not compared: for `NULL` rows their value is unspecified.
    if (const auto * expected_variant = typeid_cast<const ColumnVariant *>(&expected))
    {
        const auto & actual_variant = assert_cast<const ColumnVariant &>(actual);
        if (expected_variant->getNumVariants() != actual_variant.getNumVariants())
            return testing::AssertionFailure() << actual_variant.getNumVariants() << " variants, expected "
                                               << expected_variant->getNumVariants();
        if (auto result = samePart(
                "Variant discriminators", expected_variant->getLocalDiscriminatorsColumn(), actual_variant.getLocalDiscriminatorsColumn());
            !result)
            return result;
        for (size_t variant = 0; variant < expected_variant->getNumVariants(); ++variant)
        {
            const auto local = static_cast<ColumnVariant::Discriminator>(variant);
            if (expected_variant->globalDiscriminatorByLocal(local) != actual_variant.globalDiscriminatorByLocal(local))
                return testing::AssertionFailure() << "variant " << variant << " has another global discriminator";
            if (auto result = samePart(
                    "variant",
                    expected_variant->getVariantByLocalDiscriminator(variant),
                    actual_variant.getVariantByLocalDiscriminator(variant));
                !result)
                return result << " " << variant;
        }
        return testing::AssertionSuccess();
    }

    if (const auto * expected_dynamic = typeid_cast<const ColumnDynamic *>(&expected))
    {
        const auto & actual_dynamic = assert_cast<const ColumnDynamic &>(actual);
        if (expected_dynamic->getVariantInfo().variant_name != actual_dynamic.getVariantInfo().variant_name)
            return testing::AssertionFailure() << "variant type " << actual_dynamic.getVariantInfo().variant_name << ", expected "
                                               << expected_dynamic->getVariantInfo().variant_name;
        if (expected_dynamic->getMaxDynamicTypes() != actual_dynamic.getMaxDynamicTypes())
            return testing::AssertionFailure() << "max_types " << actual_dynamic.getMaxDynamicTypes();
        return samePart("Dynamic variant column", expected_dynamic->getVariantColumn(), actual_dynamic.getVariantColumn());
    }

    if (const auto * expected_object = typeid_cast<const ColumnObject *>(&expected))
    {
        const auto & actual_object = assert_cast<const ColumnObject &>(actual);
        const auto same_paths = [](const char * kind, const auto & expected_paths, const auto & actual_paths) -> testing::AssertionResult
        {
            if (expected_paths.size() != actual_paths.size())
                return testing::AssertionFailure() << actual_paths.size() << " " << kind << "s, expected " << expected_paths.size();
            for (const auto & [path, column] : expected_paths)
            {
                const auto it = actual_paths.find(path);
                if (it == actual_paths.end())
                    return testing::AssertionFailure() << "no " << kind << " " << path;
                if (auto result = samePart(kind, *column, *it->second); !result)
                    return result << " " << path;
            }
            return testing::AssertionSuccess();
        };
        if (auto result = same_paths("typed path", expected_object->getTypedPaths(), actual_object.getTypedPaths()); !result)
            return result;
        if (auto result = same_paths("dynamic path", expected_object->getDynamicPaths(), actual_object.getDynamicPaths()); !result)
            return result;
        return samePart("Object shared data", *expected_object->getSharedDataPtr(), *actual_object.getSharedDataPtr());
    }

    if (const auto * expected_function = typeid_cast<const ColumnFunction *>(&expected))
    {
        const auto & actual_function = assert_cast<const ColumnFunction &>(actual);
        if (expected_function->getFunction().get() != actual_function.getFunction().get())
            return testing::AssertionFailure() << "another function";
        const auto & expected_captures = expected_function->getCapturedColumns();
        const auto & actual_captures = actual_function.getCapturedColumns();
        if (expected_captures.size() != actual_captures.size())
            return testing::AssertionFailure() << actual_captures.size() << " captures, expected " << expected_captures.size();
        for (size_t capture = 0; capture < expected_captures.size(); ++capture)
        {
            if (expected_captures[capture].name != actual_captures[capture].name
                || !expected_captures[capture].type->equals(*actual_captures[capture].type))
                return testing::AssertionFailure() << "capture " << capture << " is " << actual_captures[capture].dumpStructure();
            if (auto result = samePart("Function capture", *expected_captures[capture].column, *actual_captures[capture].column); !result)
                return result << " " << capture;
        }
        return testing::AssertionSuccess();
    }

    if (const auto * expected_qbit = typeid_cast<const ColumnQBit *>(&expected))
    {
        const auto & actual_qbit = assert_cast<const ColumnQBit &>(actual);
        if (expected_qbit->getDimension() != actual_qbit.getDimension() || expected_qbit->getStride() != actual_qbit.getStride())
            return testing::AssertionFailure() << "QBit dimension " << actual_qbit.getDimension() << " and stride " << actual_qbit.getStride();
        return samePart("QBit tuple", expected_qbit->getTupleColumn(), actual_qbit.getTupleColumn());
    }

    /// `Nothing` and `Set` hold no values to compare.
    if (dynamic_cast<const IColumnDummy *>(&expected))
        return testing::AssertionSuccess();

    if (expected.isFixedAndContiguous())
        return sameBytes(expected.getRawData(), actual.getRawData());

    return testing::AssertionFailure() << "no comparison for " << expected.getFamilyName();
}

/// What a piece must equal.
enum class PieceOracle : uint8_t
{
    /// The `insertFrom` loop of `IColumn::scatter`: `cloneEmpty`, then one `insertFrom` per row, in row order.
    InsertFrom,
    /// A top-level `LowCardinality`: the source dictionary, shared, with the indexes of the piece's rows.
    SharedDictionary,
    /// A type with its own `scatter`: `filter` with the mask of the piece.
    Filter,
    /// `AggregateFunction`: views that point to the source states.
    SourceStates,
};

/// The selector cases a spec runs.
enum class CaseSet : uint8_t
{
    /// The cases with `in_short_set`.
    Short,
    /// The cases with `in_capped_set`.
    Capped,
    Every,
};

struct SourceSpec
{
    String name;
    std::function<ColumnPtr(size_t rows)> make;
    PieceOracle oracle = PieceOracle::InsertFrom;
    /// A column that is slow to clone or to filter runs the capped set, and one that is cheap to clone runs every case.
    CaseSet cases = CaseSet::Capped;
    /// The column holds a `LowCardinality` below the top level, and each piece must get its own dictionary.
    bool own_dictionaries = false;
    /// Must hold for a source of at least 3 rows, so that the case takes the path it is named after.
    std::function<bool(const IColumn & source)> on_path = {};
    /// A check of one piece beyond `oracle`.
    std::function<testing::AssertionResult(const IColumn & source, const IColumn & piece, std::span<const UInt32> rows)> extra = {};
};

template <typename Id>
bool runsCase(const SourceSpec & spec, const SelectorCase<Id> & selector_case)
{
    if (spec.cases == CaseSet::Every)
        return true;
    if (spec.cases == CaseSet::Short)
        return selector_case.in_short_set;
    return selector_case.in_capped_set;
}

/// Whether a piece is compared in full: every piece with rows, the first 1024 and the last one. The other empty
/// pieces are only checked for their representation.
bool comparedInFull(size_t piece, size_t num_pieces, size_t piece_size)
{
    /// Keeps the cases with 65536 pieces fast.
    return piece_size != 0 || piece < 1024 || piece + 1 == num_pieces;
}

/// The piece that the `insertFrom` loop of `IColumn::scatter` gives. Every kernel must give the same piece.
MutableColumnPtr insertFromPiece(const IColumn & source, std::span<const UInt32> rows)
{
    auto piece = source.cloneEmpty();
    for (const auto row : rows)
        piece->insertFrom(source, row);
    return piece;
}

Columns expectedPieces(PieceOracle oracle, const IColumn & source, const PieceRows & piece_rows)
{
    const size_t num_pieces = piece_rows.size();
    Columns result(num_pieces);
    if (oracle == PieceOracle::SourceStates)
        return result;

    IColumn::Filter mask;
    mask.resize_fill(source.size(), 0);

    for (size_t piece = 0; piece < num_pieces; ++piece)
    {
        const auto rows = piece_rows.of(piece);
        if (!comparedInFull(piece, num_pieces, rows.size()))
            continue;

        if (oracle == PieceOracle::InsertFrom)
        {
            result[piece] = insertFromPiece(source, rows);
            continue;
        }

        for (const auto row : rows)
            mask[row] = 1;

        if (oracle == PieceOracle::Filter)
        {
            result[piece] = source.filter(mask, -1);
        }
        else
        {
            const auto & low_cardinality = assert_cast<const ColumnLowCardinality &>(source);
            result[piece]
                = ColumnLowCardinality::create(low_cardinality.getDictionaryPtr(), low_cardinality.getIndexes().filter(mask, -1), true);
        }

        for (const auto row : rows)
            mask[row] = 0;
    }
    return result;
}

testing::AssertionResult sameRepresentation(const IColumn & source, const IColumn & piece)
{
    if (std::strcmp(source.getFamilyName(), piece.getFamilyName()) != 0 || source.isConst() != piece.isConst()
        || source.isSparse() != piece.isSparse() || source.isReplicated() != piece.isReplicated())
        return testing::AssertionFailure() << "representation " << piece.dumpStructure() << ", source " << source.dumpStructure();
    return testing::AssertionSuccess();
}

testing::AssertionResult pointsToSourceStates(const IColumn & source, const IColumn & piece, std::span<const UInt32> rows)
{
    const auto & source_states = assert_cast<const ColumnAggregateFunction &>(source);
    const auto * piece_states = typeid_cast<const ColumnAggregateFunction *>(&piece);
    if (!piece_states)
        return testing::AssertionFailure() << "family " << piece.getFamilyName() << ", expected AggregateFunction";
    if (piece_states->getAggregateFunction().get() != source_states.getAggregateFunction().get())
        return testing::AssertionFailure() << "another aggregate function";
    for (size_t row = 0; row < rows.size(); ++row)
    {
        if (piece_states->getData()[row] != source_states.getData()[rows[row]])
            return testing::AssertionFailure() << "row " << row << " does not point to the state of source row " << rows[row];
    }
    return testing::AssertionSuccess();
}

const IColumn * findDictionary(const IColumn & column)
{
    if (const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(&column))
        return low_cardinality->getDictionaryPtr().get();

    const IColumn * dictionary = nullptr;
    column.forEachSubcolumnRecursively(
        [&](const IColumn & subcolumn)
        {
            const auto * low_cardinality = typeid_cast<const ColumnLowCardinality *>(&subcolumn);
            if (low_cardinality && !dictionary)
                dictionary = low_cardinality->getDictionaryPtr().get();
        });
    return dictionary;
}

testing::AssertionResult samePieces(
    const SourceSpec & spec,
    const IColumn & source,
    const PieceRows & piece_rows,
    const Columns & expected,
    const VectorWithMemoryTracking<MutableColumnPtr> & pieces)
{
    const size_t num_pieces = piece_rows.size();
    if (pieces.size() != num_pieces)
        return testing::AssertionFailure() << pieces.size() << " pieces, expected " << num_pieces;

    std::unordered_set<const IColumn *> dictionaries;
    for (size_t piece = 0; piece < num_pieces; ++piece)
    {
        const IColumn & actual = *pieces[piece];
        const auto rows = piece_rows.of(piece);
        if (actual.size() != rows.size())
            return testing::AssertionFailure() << "piece " << piece << " has " << actual.size() << " rows, expected " << rows.size();

        testing::AssertionResult result = testing::AssertionSuccess();
        if (spec.oracle == PieceOracle::SourceStates)
            result = pointsToSourceStates(source, actual, rows);
        else if (expected[piece])
            result = sameColumn(*expected[piece], actual, piece < 1024 || piece + 1 == num_pieces);
        else
            result = sameRepresentation(source, actual);

        if (result && spec.extra)
            result = spec.extra(source, actual, rows);
        if (!result)
            return result << " in piece " << piece;

        if (spec.oracle == PieceOracle::SharedDictionary || spec.own_dictionaries)
            dictionaries.insert(findDictionary(actual));
    }

    if (spec.oracle == PieceOracle::SharedDictionary && dictionaries.size() > 1)
        return testing::AssertionFailure() << "the pieces hold " << dictionaries.size() << " dictionaries instead of sharing one";
    if (spec.own_dictionaries && (dictionaries.size() != num_pieces || dictionaries.contains(findDictionary(source))))
        return testing::AssertionFailure() << "the pieces share a dictionary, with each other or with the source";
    return testing::AssertionSuccess();
}

ColumnPtr makeFromFields(const String & type_name, size_t rows, const std::function<Field(size_t)> & row_value)
{
    auto column = DataTypeFactory::instance().get(type_name)->createColumn();
    for (size_t row = 0; row < rows; ++row)
        column->insert(row_value(row));
    return column;
}

ColumnPtr makeNumbers(size_t rows)
{
    auto column = ColumnUInt64::create();
    for (size_t row = 0; row < rows; ++row)
        column->insertValue(row);
    return column;
}

ColumnPtr makeStrings(size_t rows)
{
    auto column = ColumnString::create();
    for (size_t row = 0; row < rows; ++row)
    {
        String value(row % 7, static_cast<char>('a' + row % 26));
        if (row % 5 == 0)
            value += "longer than sixteen bytes";
        column->insertData(value.data(), value.size());
    }
    return column;
}

ColumnPtr makeConst(size_t rows)
{
    auto data = ColumnUInt64::create();
    data->insertValue(42);
    return ColumnConst::create(std::move(data), rows);
}

/// Every third row holds a value. The other rows read `values[0]`, which is `default_slot`.
ColumnPtr makeSparse(size_t rows, UInt64 default_slot)
{
    auto values = ColumnUInt64::create();
    auto offsets = ColumnUInt64::create();
    values->insertValue(default_slot);
    for (size_t row = 0; row < rows; ++row)
    {
        if (row % 3 == 1)
        {
            values->insertValue(row + 1);
            offsets->insertValue(row);
        }
    }
    MutableColumnPtr values_column = std::move(values);
    MutableColumnPtr offsets_column = std::move(offsets);
    return ColumnSparse::create(std::move(values_column), std::move(offsets_column), rows);
}

ColumnPtr makeReplicated(size_t rows)
{
    auto nested = ColumnString::create();
    for (const std::string_view value : {"", "a", "bb", "a value longer than sixteen bytes", "z"})
        nested->insertData(value.data(), value.size());
    auto indexes = ColumnUInt8::create();
    for (size_t row = 0; row < rows; ++row)
        indexes->insertValue(static_cast<UInt8>(row * 3 % 5));

    MutableColumnPtr nested_column = std::move(nested);
    MutableColumnPtr indexes_column = std::move(indexes);
    return ColumnReplicated::create(std::move(nested_column), std::move(indexes_column));
}

/// 300 values and the default make 301 dictionary entries, so the indexes are `UInt16` at any row count.
ColumnPtr makeLowCardinalityWithWideIndexes(size_t rows)
{
    DataTypeString string_type;
    auto dictionary = ColumnUnique<ColumnString>::create(string_type);
    for (size_t value = 0; value < 300; ++value)
        dictionary->uniqueInsert(Field("value " + std::to_string(value)));
    auto indexes = ColumnUInt16::create();
    for (size_t row = 0; row < rows; ++row)
        indexes->insertValue(static_cast<UInt16>(row * 7 % 301));

    MutableColumnPtr dictionary_column = std::move(dictionary);
    MutableColumnPtr indexes_column = std::move(indexes);
    return ColumnLowCardinality::create(std::move(dictionary_column), std::move(indexes_column), false);
}

ColumnPtr makeDynamic(size_t rows)
{
    /// At most two dynamic types. `Float64`, the third type to appear, goes to the shared variant.
    auto column = ColumnDynamic::create(2);
    for (size_t row = 0; row < rows; ++row)
    {
        switch (row % 4)
        {
            case 0: column->insert(Field(static_cast<Int64>(row % 100))); break;
            case 1: column->insert(Field("d" + std::to_string(row))); break;
            case 2: column->insert(Field(static_cast<Float64>(row) + 0.5)); break;
            default: column->insert(Field(Null())); break;
        }
    }
    return column;
}

ColumnPtr makeObject(size_t rows)
{
    /// `a.b` and `a.c` are typed paths, `d` takes the only dynamic path, `s1` and `s2` go to the shared data.
    return makeFromFields(
        "JSON(max_dynamic_paths=1, a.b UInt32, a.c Array(String))",
        rows,
        [](size_t row)
        {
            Object object;
            object["a.b"] = Field(static_cast<UInt64>(row));
            object["a.c"] = Field(Array{Field("x" + std::to_string(row % 3))});
            object["d"] = Field(static_cast<Int64>(row));
            if (row % 2 == 0)
                object["s1"] = Field("v" + std::to_string(row));
            if (row % 3 == 0)
                object["s2"] = Field(static_cast<Float64>(row) / 2);
            return Field(object);
        });
}

/// A function that is only captured, never executed.
class FunctionCaptureOnly final : public IFunction
{
public:
    explicit FunctionCaptureOnly(size_t number_of_arguments_)
        : number_of_arguments(number_of_arguments_)
    {
    }

    String getName() const override { return "captureOnly"; }
    size_t getNumberOfArguments() const override { return number_of_arguments; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        return result_type->createColumnConstWithDefaultValue(input_rows_count);
    }

private:
    size_t number_of_arguments;
};

FunctionBasePtr makeCaptureOnlyFunction(const DataTypes & argument_types)
{
    return std::make_shared<FunctionToFunctionBaseAdaptor>(
        std::make_shared<FunctionCaptureOnly>(argument_types.size()), argument_types, std::make_shared<DataTypeUInt8>());
}

ColumnPtr makeFunctionWithCaptures(size_t rows)
{
    static const DataTypes argument_types{
        std::make_shared<DataTypeUInt64>(),
        std::make_shared<DataTypeString>(),
        DataTypeFactory::instance().get("Nullable(UInt64)"),
        std::make_shared<DataTypeUInt64>()};
    static const FunctionBasePtr function = makeCaptureOnlyFunction(argument_types);

    const auto nullable = makeFromFields(
        "Nullable(UInt64)", rows, [](size_t row) { return row % 4 == 0 ? Field(Null()) : Field(static_cast<UInt64>(row)); });
    const ColumnsWithTypeAndName captures{
        {makeConst(rows), argument_types[0], "constant"},
        {makeStrings(rows), argument_types[1], "string"},
        {nullable, argument_types[2], "nullable"}};
    return ColumnFunction::create(rows, function, captures);
}

ColumnPtr makeFunctionWithoutCaptures(size_t rows)
{
    static const FunctionBasePtr function = makeCaptureOnlyFunction({std::make_shared<DataTypeUInt64>()});
    return ColumnFunction::create(rows, function, ColumnsWithTypeAndName{});
}

ColumnPtr makeAggregateStates(size_t rows)
{
    tryRegisterAggregateFunctions();
    DataTypes argument_types = {std::make_shared<DataTypeUInt64>()};
    Array parameters;
    AggregateFunctionProperties properties;
    const auto function = AggregateFunctionFactory::instance().get("sum", NullsAction::EMPTY, argument_types, parameters, properties);

    auto column = ColumnAggregateFunction::create(function);
    auto values = ColumnUInt64::create();
    for (size_t row = 0; row < rows; ++row)
        values->insertValue(row * 3 + 1);
    const IColumn * arguments[] = {values.get()};
    for (size_t row = 0; row < rows; ++row)
    {
        column->insertDefault();
        function->add(column->getData().back(), arguments, row, &column->createOrGetArena());
    }
    return column;
}

testing::AssertionResult sharesConstData(const IColumn & source, const IColumn & piece, std::span<const UInt32>)
{
    if (assert_cast<const ColumnConst &>(piece).getDataColumnPtr().get()
        != assert_cast<const ColumnConst &>(source).getDataColumnPtr().get())
        return testing::AssertionFailure() << "the piece does not share the Const data";
    return testing::AssertionSuccess();
}

/// A piece gets a fresh `values[0]`, so the rows that read the source's non-default `values[0]` read the type default.
testing::AssertionResult readsTypeDefault(const IColumn & source, const IColumn & piece, std::span<const UInt32> rows)
{
    for (size_t row = 0; row < rows.size(); ++row)
    {
        if (source.isDefaultAt(rows[row]) && piece[row] != Field(static_cast<UInt64>(0)))
            return testing::AssertionFailure() << "row " << row << " reads " << piece[row].dump() << ", expected 0";
    }
    return testing::AssertionSuccess();
}

std::vector<SourceSpec> makeSourceSpecs()
{
    std::vector<SourceSpec> specs;

    specs.push_back({.name = "Const(UInt64)", .make = makeConst, .cases = CaseSet::Every, .extra = sharesConstData});
    specs.push_back(
        {.name = "Nothing", .make = [](size_t rows) -> ColumnPtr { return ColumnNothing::create(rows); }, .cases = CaseSet::Every});
    specs.push_back(
        {.name = "Set", .make = [](size_t rows) -> ColumnPtr { return ColumnSet::create(rows, FutureSetPtr{}); }, .cases = CaseSet::Every});

    specs.push_back({.name = "Sparse(UInt64)", .make = [](size_t rows) { return makeSparse(rows, 0); }});
    specs.push_back({
        .name = "Sparse(UInt64) with a non-default values[0]",
        .make = [](size_t rows) { return makeSparse(rows, 7); },
        .on_path = [](const IColumn & source) { return assert_cast<const ColumnSparse &>(source).getValuesColumn().getUInt(0) == 7; },
        .extra = readsTypeDefault,
    });
    specs.push_back({.name = "Replicated(String)", .make = makeReplicated});
    specs.push_back({
        .name = "LowCardinality(String) with UInt8 indexes",
        .make = [](size_t rows)
        { return makeFromFields("LowCardinality(String)", rows, [](size_t row) { return Field("s" + std::to_string(row % 50)); }); },
        .oracle = PieceOracle::SharedDictionary,
        .on_path = [](const IColumn & source) { return assert_cast<const ColumnLowCardinality &>(source).getSizeOfIndexType() == 1; },
    });
    specs.push_back({
        .name = "LowCardinality(String) with UInt16 indexes",
        .make = makeLowCardinalityWithWideIndexes,
        .oracle = PieceOracle::SharedDictionary,
        .on_path = [](const IColumn & source) { return assert_cast<const ColumnLowCardinality &>(source).getSizeOfIndexType() == 2; },
    });
    specs.push_back({
        .name = "Tuple(Sparse(UInt64))",
        .make = [](size_t rows) -> ColumnPtr { return ColumnTuple::create(Columns{makeSparse(rows, 0)}); },
    });
    specs.push_back({
        .name = "Array(LowCardinality(String))",
        .make =
            [](size_t rows)
        {
            return makeFromFields(
                "Array(LowCardinality(String))",
                rows,
                [](size_t row)
                {
                    Array array;
                    for (size_t element = 0; element < row % 4; ++element)
                        array.push_back(Field("e" + std::to_string((row * 3 + element) % 11)));
                    return Field(array);
                });
        },
        .own_dictionaries = true,
    });
    specs.push_back({
        .name = "Map(LowCardinality(String), UInt64)",
        .make =
            [](size_t rows)
        {
            return makeFromFields(
                "Map(LowCardinality(String), UInt64)",
                rows,
                [](size_t row)
                {
                    Map map;
                    for (size_t entry = 0; entry < row % 3; ++entry)
                        map.push_back(Tuple{Field("k" + std::to_string((row + entry) % 7)), Field(static_cast<UInt64>(row * 10 + entry))});
                    return Field(map);
                });
        },
        .own_dictionaries = true,
    });
    specs.push_back({
        .name = "Nullable(Tuple(LowCardinality(String), UInt64))",
        .make =
            [](size_t rows)
        {
            return makeFromFields(
                "Nullable(Tuple(LowCardinality(String), UInt64))",
                rows,
                [](size_t row)
                {
                    if (row % 5 == 4)
                        return Field(Null());
                    return Field(Tuple{Field("t" + std::to_string(row % 13)), Field(static_cast<UInt64>(row))});
                });
        },
        .own_dictionaries = true,
    });

    /// The three paths of `ColumnVariant::scatter`.
    specs.push_back({
        .name = "Variant with only NULLs",
        .make = [](size_t rows) { return makeFromFields("Variant(String, UInt64)", rows, [](size_t) { return Field(Null()); }); },
        .oracle = PieceOracle::Filter,
        .on_path = [](const IColumn & source) { return assert_cast<const ColumnVariant &>(source).hasOnlyNulls(); },
    });
    specs.push_back({
        .name = "Variant with one non-empty variant and no NULLs",
        .make = [](size_t rows)
        { return makeFromFields("Variant(String, UInt64)", rows, [](size_t row) { return Field(static_cast<UInt64>(row)); }); },
        .oracle = PieceOracle::Filter,
        .on_path = [](const IColumn & source)
        { return assert_cast<const ColumnVariant &>(source).getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls().has_value(); },
    });
    specs.push_back({
        .name = "Variant with several variants and NULLs",
        .make =
            [](size_t rows)
        {
            return makeFromFields(
                "Variant(String, UInt64)",
                rows,
                [](size_t row)
                {
                    if (row % 3 == 0)
                        return Field(static_cast<UInt64>(row));
                    if (row % 3 == 1)
                        return Field("v" + std::to_string(row));
                    return Field(Null());
                });
        },
        .oracle = PieceOracle::Filter,
        .on_path =
            [](const IColumn & source)
        {
            const auto & variant = assert_cast<const ColumnVariant &>(source);
            return !variant.hasOnlyNulls() && !variant.getLocalDiscriminatorOfOneNoneEmptyVariantNoNulls();
        },
    });
    specs.push_back({
        .name = "Dynamic with shared-variant rows",
        .make = makeDynamic,
        .oracle = PieceOracle::Filter,
        .on_path = [](const IColumn & source) { return !assert_cast<const ColumnDynamic &>(source).getSharedVariant().empty(); },
    });
    specs.push_back({
        .name = "Object with typed paths, a dynamic path and shared data",
        .make = makeObject,
        .oracle = PieceOracle::Filter,
        .on_path =
            [](const IColumn & source)
        {
            const auto & object = assert_cast<const ColumnObject &>(source);
            return object.getTypedPaths().size() == 2 && object.getDynamicPaths().size() == 1
                && !object.getSharedDataPathsAndValues().first->empty();
        },
    });
    specs.push_back({.name = "Function with Const and other captures", .make = makeFunctionWithCaptures, .oracle = PieceOracle::Filter});
    specs.push_back({.name = "Function without captures", .make = makeFunctionWithoutCaptures, .oracle = PieceOracle::Filter});
    specs.push_back({.name = "AggregateFunction(sum, UInt64)", .make = makeAggregateStates, .oracle = PieceOracle::SourceStates});

    return specs;
}

const std::vector<SourceSpec> & sourceSpecs()
{
    static const std::vector<SourceSpec> specs = makeSourceSpecs();
    return specs;
}

template <typename F>
void expectErrorCode(int code, const char * what, F && call)
{
    try
    {
        call();
    }
    catch (const Exception & e)
    {
        EXPECT_EQ(e.code(), code) << what << ": " << e.message();
        return;
    }
    ADD_FAILURE() << what << ": no exception, expected error code " << code;
}

template <typename Id>
void expectScatterThrows(const IColumn & column, int code)
{
    PaddedPODArray<Id> selector;
    for (size_t row = 0; row < column.size(); ++row)
        selector.push_back(static_cast<Id>(row % 3));
    const auto counts = countColumnsSizeInSelector(3, selector);

    expectErrorCode(code, column.getFamilyName(), [&] { (void)column.scatter(3, selector); });
    expectErrorCode(code, column.getFamilyName(), [&] { (void)column.scatterWithCounts(selector, counts); });
    const std::array<const IColumn *, 1> sources{&column};
    const std::array<std::span<const Id>, 1> ids{std::span<const Id>(selector.data(), selector.size())};
    expectErrorCode(code, column.getFamilyName(), [&] { (void)ColumnsScatter::scatter<Id>(sources, ids, counts); });
}

/// The columns that cannot be scattered throw these error codes from `scatter`, `scatterWithCounts` and the batched
/// `ColumnsScatter::scatter`.
template <typename Id>
void expectThrowingColumns()
{
    const size_t rows = 16;
    expectScatterThrows<Id>(*ColumnLazy::create(rows), ErrorCodes::NOT_IMPLEMENTED);

    DataTypeString string_type;
    expectScatterThrows<Id>(*ColumnUnique<ColumnString>::create(string_type), ErrorCodes::NOT_IMPLEMENTED);

#if !defined(DEBUG_OR_SANITIZER_BUILD)
    /// These two throw `LOGICAL_ERROR`, which aborts Debug and sanitizer builds.
    const auto blob = ColumnBLOB::create([](const ColumnBLOB::BLOB &) -> ColumnPtr { return {}; }, makeNumbers(rows), rows);
    expectScatterThrows<Id>(*blob, ErrorCodes::LOGICAL_ERROR);
    expectScatterThrows<Id>(*ColumnCompressed::wrap(makeNumbers(rows)), ErrorCodes::LOGICAL_ERROR);
#endif
}

using ColumnMaker = std::function<MutableColumnPtr(size_t rows)>;

/// Makes `Column::create(args...)` with `rows` values of random bytes.
template <typename Column, typename... Args>
ColumnMaker randomFixed(Args... args)
{
    return [args...](size_t rows) -> MutableColumnPtr
    {
        auto column = Column::create(args...);
        pcg64 rng(selector_seed + rows);
        for (char & byte : column->insertRawUninitialized(rows))
            byte = static_cast<char>(rng());
        return column;
    };
}

/// Empty strings, short ones, strings of exactly 16 bytes and longer ones.
MutableColumnPtr makeMixedStrings(size_t rows)
{
    static constexpr std::array<size_t, 7> lengths = {0, 1, 7, 15, 16, 17, 40};
    auto column = ColumnString::create();
    String value;
    for (size_t row = 0; row < rows; ++row)
    {
        value.resize(lengths[row * 5 % lengths.size()]);
        for (size_t position = 0; position < value.size(); ++position)
            value[position] = static_cast<char>('a' + (row + position) % 26);
        column->insertData(value.data(), value.size());
    }
    return column;
}

MutableColumnPtr makeEmptyStrings(size_t rows)
{
    auto column = ColumnString::create();
    column->insertManyDefaults(rows);
    return column;
}

/// Every third row is `NULL`.
ColumnMaker nullableOf(ColumnMaker nested)
{
    return [nested](size_t rows) -> MutableColumnPtr
    {
        auto null_map = ColumnUInt8::create();
        for (size_t row = 0; row < rows; ++row)
            null_map->insertValue(static_cast<UInt8>(row % 3 == 1));
        return ColumnNullable::create(nested(rows), std::move(null_map));
    };
}

/// Each row holds 0 to `max_length` elements, so some arrays are empty.
ColumnMaker arrayOf(ColumnMaker nested, size_t max_length = 4)
{
    return [nested, max_length](size_t rows) -> MutableColumnPtr
    {
        auto offsets = ColumnArray::ColumnOffsets::create();
        UInt64 elements = 0;
        for (size_t row = 0; row < rows; ++row)
        {
            elements += (row * 3 + 1) % (max_length + 1);
            offsets->insertValue(elements);
        }
        return ColumnArray::create(nested(elements), std::move(offsets));
    };
}

ColumnMaker tupleOf(std::vector<ColumnMaker> elements)
{
    return [elements](size_t rows) -> MutableColumnPtr
    {
        if (elements.empty())
            return ColumnTuple::create(rows);
        MutableColumns columns;
        for (const auto & element : elements)
            columns.push_back(element(rows));
        return ColumnTuple::create(std::move(columns));
    };
}

MutableColumnPtr makeStringToNumberMap(size_t rows)
{
    return ColumnMap::create(arrayOf(tupleOf({makeMixedStrings, randomFixed<ColumnUInt64>()}), 3)(rows));
}

/// `BFloat16` vectors of dimension 8: 16 bit planes of one byte.
MutableColumnPtr makeQBit(size_t rows)
{
    MutableColumns planes;
    for (size_t plane = 0; plane < 16; ++plane)
    {
        auto column = ColumnFixedString::create(1);
        for (size_t row = 0; row < rows; ++row)
        {
            const char value = static_cast<char>('a' + (row + plane) % 20);
            column->insertData(&value, 1);
        }
        planes.push_back(std::move(column));
    }
    MutableColumnPtr tuple = ColumnTuple::create(std::move(planes));
    return ColumnQBit::create(std::move(tuple), 8, 8);
}

/// Every type that reaches a kernel, through `hasKernel` or, for a `Map`, through its nested `Array`. `UInt64` and
/// `String` run every case. One type per kernel and per compile-time width runs the capped set. The other types share
/// a kernel with one of these and run the short set.
std::vector<SourceSpec> makeKernelSpecs()
{
    std::vector<SourceSpec> specs;
    const auto add = [&specs](String name, ColumnMaker make, CaseSet cases)
    { specs.push_back({.name = std::move(name), .make = std::move(make), .cases = cases}); };

    struct Leaf
    {
        String name;
        ColumnMaker make;
        CaseSet cases = CaseSet::Short;
        CaseSet nullable_cases = CaseSet::Short;
        CaseSet array_cases = CaseSet::Short;
    };

    std::vector<Leaf> leaves = {
        {"UInt8", randomFixed<ColumnUInt8>(), CaseSet::Capped},
        {"UInt16", randomFixed<ColumnUInt16>(), CaseSet::Capped},
        {"UInt32", randomFixed<ColumnUInt32>(), CaseSet::Capped},
        {"UInt64", randomFixed<ColumnUInt64>(), CaseSet::Every, CaseSet::Short, CaseSet::Capped},
        {"UInt128", randomFixed<ColumnUInt128>(), CaseSet::Capped},
        {"UInt256", randomFixed<ColumnUInt256>(), CaseSet::Capped},
        {"Int8", randomFixed<ColumnInt8>()},
        {"Int16", randomFixed<ColumnInt16>()},
        {"Int32", randomFixed<ColumnInt32>()},
        {"Int64", randomFixed<ColumnInt64>()},
        {"Int128", randomFixed<ColumnInt128>()},
        {"Int256", randomFixed<ColumnInt256>()},
        {"BFloat16", randomFixed<ColumnBFloat16>()},
        {"Float32", randomFixed<ColumnFloat32>()},
        {"Float64", randomFixed<ColumnFloat64>()},
        {"UUID", randomFixed<ColumnUUID>()},
        {"IPv4", randomFixed<ColumnIPv4>()},
        {"IPv6", randomFixed<ColumnIPv6>()},
        {"DateTime64(3)", randomFixed<ColumnDecimal<DateTime64>>(size_t{0}, UInt32{3})},
        {"Time64(6)", randomFixed<ColumnDecimal<Time64>>(size_t{0}, UInt32{6})},
        {"String", makeMixedStrings, CaseSet::Every, CaseSet::Capped, CaseSet::Capped},
        {"String with only empty rows", makeEmptyStrings},
    };
    for (const UInt32 scale : {UInt32{0}, UInt32{4}})
    {
        leaves.push_back({fmt::format("Decimal32({})", scale), randomFixed<ColumnDecimal<Decimal32>>(size_t{0}, scale)});
        leaves.push_back({fmt::format("Decimal64({})", scale), randomFixed<ColumnDecimal<Decimal64>>(size_t{0}, scale)});
        leaves.push_back({fmt::format("Decimal128({})", scale), randomFixed<ColumnDecimal<Decimal128>>(size_t{0}, scale)});
        leaves.push_back({fmt::format("Decimal256({})", scale), randomFixed<ColumnDecimal<Decimal256>>(size_t{0}, scale)});
    }
    /// Widths 1, 2, 4, 8, 16 and 32 are compile-time widths, and the other widths share the code for any width. The
    /// list holds widths at and next to the compile-time ones, and 100.
    for (const size_t width : std::to_array<size_t>({1, 3, 5, 7, 8, 15, 16, 17, 31, 32, 33, 100}))
    {
        leaves.push_back(
            {fmt::format("FixedString({})", width), randomFixed<ColumnFixedString>(width), width == 3 ? CaseSet::Capped : CaseSet::Short});
    }

    for (const auto & leaf : leaves)
    {
        add(leaf.name, leaf.make, leaf.cases);
        add(fmt::format("Nullable({})", leaf.name), nullableOf(leaf.make), leaf.nullable_cases);
        add(fmt::format("Array({})", leaf.name), arrayOf(leaf.make), leaf.array_cases);
    }

    add("Nullable(Tuple(UInt32, String))", nullableOf(tupleOf({randomFixed<ColumnUInt32>(), makeMixedStrings})), CaseSet::Capped);
    add("Array(Array(UInt8))", arrayOf(arrayOf(randomFixed<ColumnUInt8>())), CaseSet::Capped);
    add("Array(Nullable(String))", arrayOf(nullableOf(makeMixedStrings)), CaseSet::Short);
    add("Array(Nullable(Tuple(UInt64)))", arrayOf(nullableOf(tupleOf({randomFixed<ColumnUInt64>()}))), CaseSet::Capped);
    add("Array(Tuple(UInt32, String))", arrayOf(tupleOf({randomFixed<ColumnUInt32>(), makeMixedStrings})), CaseSet::Short);
    add("Array(Tuple())", arrayOf(tupleOf({})), CaseSet::Short);
    add("Array(UInt64) with only empty arrays", arrayOf(randomFixed<ColumnUInt64>(), 0), CaseSet::Short);
    add("Map(String, UInt64)", makeStringToNumberMap, CaseSet::Capped);
    /// An `Array` whose elements need sizes of their own, below a second `Array` or inside a `Map` value.
    add("Array(Array(String))", arrayOf(arrayOf(makeMixedStrings)), CaseSet::Capped);
    add("Array(Array(Nullable(String)))", arrayOf(arrayOf(nullableOf(makeMixedStrings))), CaseSet::Short);
    add(
        "Map(String, Array(String))",
        [](size_t rows) -> MutableColumnPtr
        { return ColumnMap::create(arrayOf(tupleOf({makeMixedStrings, arrayOf(makeMixedStrings)}), 3)(rows)); },
        CaseSet::Short);
    return specs;
}

const std::vector<SourceSpec> & kernelSpecs()
{
    static const std::vector<SourceSpec> specs = makeKernelSpecs();
    return specs;
}

/// The types that take no kernel and that `sourceSpecs` does not hold. `HasKernel` checks that they have none, and
/// `BatchedSingleSourceMatchesScatter` runs them, like every other type, on the short set.
const std::vector<SourceSpec> & otherSpecs()
{
    static const std::vector<SourceSpec> specs = {
        {.name = "Tuple(UInt32, String)", .make = tupleOf({randomFixed<ColumnUInt32>(), makeMixedStrings})},
        {.name = "QBit(BFloat16, 8)", .make = makeQBit},
    };
    return specs;
}

const std::vector<SourceSpec> & allSpecs()
{
    static const std::vector<SourceSpec> specs = []
    {
        std::vector<SourceSpec> result = kernelSpecs();
        result.insert(result.end(), sourceSpecs().begin(), sourceSpecs().end());
        result.insert(result.end(), otherSpecs().begin(), otherSpecs().end());
        return result;
    }();
    return specs;
}

/// The source of one of `sourceSpecs`, by name.
ColumnPtr makeSpecSource(std::string_view name, size_t rows)
{
    for (const auto & spec : sourceSpecs())
    {
        if (spec.name == name)
            return spec.make(rows);
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "No source spec named {}", name);
}

/// One element per row.
ColumnPtr makeOffsets(size_t rows)
{
    auto offsets = ColumnArray::ColumnOffsets::create();
    for (size_t row = 0; row < rows; ++row)
        offsets->insertValue(row + 1);
    return offsets;
}

ColumnPtr makeNoNulls(size_t rows)
{
    auto null_map = ColumnUInt8::create();
    null_map->insertManyDefaults(rows);
    return null_map;
}

ColumnPtr makeConstString(std::string_view value, size_t rows)
{
    auto data = ColumnString::create();
    data->insertData(value.data(), value.size());
    return ColumnConst::create(std::move(data), rows);
}

template <typename Column>
ColumnPtr makeConstOf(typename Column::ValueType value, size_t rows)
{
    auto data = Column::create();
    data->insertValue(value);
    return ColumnConst::create(std::move(data), rows);
}

ColumnPtr makeConstNullableString(size_t rows)
{
    auto nested = ColumnString::create();
    nested->insertData("knull", 5);
    return ColumnConst::create(ColumnNullable::create(ColumnPtr(std::move(nested)), makeNoNulls(1)), rows);
}

/// A `Const(Function)`, like the column of a lambda with only constant arguments. All of them share one function.
ColumnPtr makeConstFunction(size_t rows)
{
    static const DataTypePtr argument_type = std::make_shared<DataTypeUInt64>();
    static const FunctionBasePtr function = makeCaptureOnlyFunction({argument_type});
    auto captured = ColumnUInt64::create();
    captured->insertValue(42);
    const ColumnsWithTypeAndName captures{{std::move(captured), argument_type, "x"}};
    return ColumnConst::create(ColumnFunction::create(1, function, captures), rows);
}

/// Columns that `hasKernel` must reject and that no spec above holds: wrappers below an accepted composite, a
/// `LowCardinality` or a `Map` inside an `Array`, a wrapper that reports the `TypeIndex` of an accepted type, and
/// other types inside an `Array` or a `Nullable`.
std::vector<std::pair<String, ColumnPtr>> makeRejectedColumns(size_t rows)
{
    tryRegisterAggregateFunctions();
    std::vector<std::pair<String, ColumnPtr>> columns;
    for (const char * type_name :
         {"LowCardinality(Nullable(String))",
          "Array(LowCardinality(Nullable(String)))",
          "Array(Array(LowCardinality(String)))",
          "Array(Tuple(LowCardinality(String), UInt64))",
          "Array(Nullable(Tuple(LowCardinality(String), UInt64)))",
          "Array(Map(String, UInt64))",
          "Array(Variant(String, UInt64))",
          "Array(Dynamic)",
          "Array(Nothing)",
          "Nullable(Nothing)",
          "Array(AggregateFunction(sum, UInt64))"})
        columns.emplace_back(type_name, DataTypeFactory::instance().get(type_name)->createColumn());

    auto sparse_values = ColumnString::create();
    sparse_values->insertDefault();
    sparse_values->insertData("x", 1);
    auto sparse_offsets = ColumnUInt64::create();
    sparse_offsets->insertValue(rows / 2);
    MutableColumnPtr sparse_values_column = std::move(sparse_values);
    MutableColumnPtr sparse_offsets_column = std::move(sparse_offsets);

    columns.emplace_back("Sparse(String)", ColumnSparse::create(std::move(sparse_values_column), std::move(sparse_offsets_column), rows));
    columns.emplace_back("Const(String)", makeConstString("value", rows));
    columns.emplace_back("Const(Nullable(String))", makeConstNullableString(rows));
    columns.emplace_back("Const(Array(UInt64))", ColumnConst::create(ColumnArray::create(makeNumbers(1), makeOffsets(1)), rows));
    columns.emplace_back("Array(Sparse(UInt64))", ColumnArray::create(makeSparse(rows, 0), makeOffsets(rows)));
    columns.emplace_back("Array(Replicated(String))", ColumnArray::create(makeReplicated(rows), makeOffsets(rows)));
    columns.emplace_back(
        "Array(Tuple(UInt64, Sparse(UInt64)))",
        ColumnArray::create(ColumnTuple::create(Columns{makeNumbers(rows), makeSparse(rows, 0)}), makeOffsets(rows)));
    columns.emplace_back(
        "Nullable(Tuple(UInt64, Sparse(UInt64)))",
        ColumnNullable::create(ColumnTuple::create(Columns{makeNumbers(rows), makeSparse(rows, 0)}), makeNoNulls(rows)));

    DataTypeString string_type;
    columns.emplace_back("ColumnUnique(String), which reports the TypeIndex of String", ColumnUnique<ColumnString>::create(string_type));
    columns.emplace_back("Lazy", ColumnLazy::create(rows));
    return columns;
}

/// One array of a column, as the exact-size checks see it.
struct ArrayState
{
    bool holds_heap_block = false;
    size_t allocated_bytes = 0;
    size_t size = 0;
    size_t capacity = 0;
};

/// An array without a heap block points into `empty_pod_array`.
bool holdsHeapBlock(const void * data)
{
    const auto address = reinterpret_cast<uintptr_t>(data);
    const auto empty_begin = reinterpret_cast<uintptr_t>(empty_pod_array);
    return address < empty_begin || address >= empty_begin + empty_pod_array_size;
}

template <typename Array>
ArrayState arrayState(const Array & array)
{
    return {holdsHeapBlock(array.data()), array.allocated_bytes(), array.size(), array.capacity()};
}

/// Appends the arrays of a column of a kernel type, part by part.
testing::AssertionResult collectArrays(const IColumn & column, std::vector<ArrayState> & arrays)
{
    if (const auto * string = typeid_cast<const ColumnString *>(&column))
    {
        arrays.push_back(arrayState(string->getOffsets()));
        arrays.push_back(arrayState(string->getChars()));
        return testing::AssertionSuccess();
    }
    if (const auto * fixed_string = typeid_cast<const ColumnFixedString *>(&column))
    {
        arrays.push_back(arrayState(fixed_string->getChars()));
        return testing::AssertionSuccess();
    }
    if (const auto * nullable = typeid_cast<const ColumnNullable *>(&column))
    {
        if (auto result = collectArrays(nullable->getNullMapColumn(), arrays); !result)
            return result;
        return collectArrays(nullable->getNestedColumn(), arrays);
    }
    if (const auto * array = typeid_cast<const ColumnArray *>(&column))
    {
        if (auto result = collectArrays(array->getOffsetsColumn(), arrays); !result)
            return result;
        return collectArrays(array->getData(), arrays);
    }
    if (const auto * tuple = typeid_cast<const ColumnTuple *>(&column))
    {
        for (size_t element = 0; element < tuple->tupleSize(); ++element)
        {
            if (auto result = collectArrays(tuple->getColumn(element), arrays); !result)
                return result;
        }
        return testing::AssertionSuccess();
    }
    if (const auto * map = typeid_cast<const ColumnMap *>(&column))
        return collectArrays(map->getNestedColumn(), arrays);

    /// `ColumnVector` and `ColumnDecimal`.
    if (!column.isFixedAndContiguous())
        return testing::AssertionFailure() << "no exact-size check for " << column.getFamilyName();
    arrays.push_back({holdsHeapBlock(column.getRawData().data()), column.allocatedBytes(), column.size(), column.capacity()});
    return testing::AssertionSuccess();
}

/// `piece` has its exact size. An array with elements has no spare capacity. An array without elements has the bytes
/// of the same array of `oracle`, and a heap block exactly when that array has one. The piece holds at most the bytes
/// of `oracle`. For a piece without rows, `oracle` is the source's `cloneEmpty`.
testing::AssertionResult exactPiece(const IColumn & piece, const IColumn & oracle)
{
    std::vector<ArrayState> actual;
    std::vector<ArrayState> expected;
    if (auto result = collectArrays(piece, actual); !result)
        return result;
    if (auto result = collectArrays(oracle, expected); !result)
        return result;
    if (actual.size() != expected.size())
        return testing::AssertionFailure() << actual.size() << " arrays, expected " << expected.size();

    for (size_t index = 0; index < actual.size(); ++index)
    {
        const auto & array = actual[index];
        if (piece.empty() || array.size == 0)
        {
            if (array.holds_heap_block != expected[index].holds_heap_block || array.allocated_bytes != expected[index].allocated_bytes)
                return testing::AssertionFailure() << "array " << index << " holds " << array.allocated_bytes << " bytes"
                                                   << (array.holds_heap_block ? " in a heap block" : "") << ", expected "
                                                   << expected[index].allocated_bytes
                                                   << (expected[index].holds_heap_block ? " in a heap block" : "");
        }
        else if (array.capacity != array.size)
        {
            return testing::AssertionFailure() << "array " << index << " has capacity " << array.capacity << " for " << array.size
                                               << " elements";
        }
    }

    if (piece.allocatedBytes() > oracle.allocatedBytes())
        return testing::AssertionFailure() << "the piece holds " << piece.allocatedBytes() << " bytes, expected at most "
                                           << oracle.allocatedBytes();
    return testing::AssertionSuccess();
}

Columns toColumns(VectorWithMemoryTracking<MutableColumnPtr> && pieces)
{
    Columns columns;
    columns.reserve(pieces.size());
    for (auto & piece : pieces)
        columns.push_back(std::move(piece));
    return columns;
}

template <typename Id>
PaddedPODArray<Id> randomIds(pcg64 & rng, size_t rows, size_t num_pieces)
{
    PaddedPODArray<Id> ids;
    ids.reserve_exact(rows);
    for (size_t row = 0; row < rows; ++row)
        ids.push_back(static_cast<Id>(rng() % num_pieces));
    return ids;
}

template <typename Id>
PaddedPODArray<Id> sliceOf(const PaddedPODArray<Id> & ids, size_t begin, size_t end)
{
    PaddedPODArray<Id> slice;
    slice.insert(ids.begin() + begin, ids.begin() + end);
    return slice;
}

/// The sources of one column position, and the piece ids of their rows.
template <typename Id>
struct Batch
{
    std::vector<ColumnPtr> sources;
    std::vector<PaddedPODArray<Id>> ids;

    void add(ColumnPtr source, PaddedPODArray<Id> source_ids)
    {
        sources.push_back(std::move(source));
        ids.push_back(std::move(source_ids));
    }
};

struct BatchResult
{
    VectorWithMemoryTracking<size_t> counts;
    VectorWithMemoryTracking<MutableColumnPtr> pieces;
};

/// Calls the batched `ColumnsScatter::scatter` with the counts of `countRowsPerPiece`.
template <typename Id>
BatchResult scatterBatch(const Batch<Id> & batch, size_t num_pieces, bool allow_combining = false)
{
    std::vector<const IColumn *> sources;
    std::vector<std::span<const Id>> ids;
    for (size_t index = 0; index < batch.sources.size(); ++index)
    {
        sources.push_back(batch.sources[index].get());
        ids.emplace_back(batch.ids[index].data(), batch.ids[index].size());
    }

    BatchResult result;
    result.counts = ColumnsScatter::countRowsPerPiece<Id>(ids, num_pieces);
    result.pieces = ColumnsScatter::scatter<Id>(sources, ids, result.counts, allow_combining);
    return result;
}

/// What the batched `ColumnsScatter::scatter` must give for several sources. Each source is scattered on its own,
/// without its wrappers, and each piece appends its parts in source order with `insertRangeFrom`.
template <typename Id>
Columns perSourcePieces(const Batch<Id> & batch, size_t num_pieces)
{
    Columns normalized;
    for (const auto & source : batch.sources)
        normalized.push_back(source->convertToFullIfWrapped());

    MutableColumns pieces(num_pieces);
    for (auto & piece : pieces)
        piece = normalized.front()->cloneEmpty();
    for (size_t index = 0; index < normalized.size(); ++index)
    {
        const auto parts = normalized[index]->scatter(num_pieces, batch.ids[index]);
        for (size_t piece = 0; piece < num_pieces; ++piece)
            pieces[piece]->insertRangeFrom(*parts[piece], 0, parts[piece]->size());
    }

    Columns result;
    for (auto & piece : pieces)
        result.push_back(std::move(piece));
    return result;
}

/// The batched `ColumnsScatter::scatter` gives the pieces of `perSourcePieces`. With `exact`, each piece also has its
/// exact size.
template <typename Id>
testing::AssertionResult sameAsPerSource(const Batch<Id> & batch, size_t num_pieces, bool exact, bool allow_combining = false)
{
    const auto expected = perSourcePieces(batch, num_pieces);
    const auto actual = scatterBatch(batch, num_pieces, allow_combining);
    if (actual.pieces.size() != num_pieces)
        return testing::AssertionFailure() << actual.pieces.size() << " pieces, expected " << num_pieces;

    for (size_t piece = 0; piece < num_pieces; ++piece)
    {
        auto result = sameColumn(*expected[piece], *actual.pieces[piece], piece < 1024 || piece + 1 == num_pieces);
        if (result && exact)
            result = exactPiece(*actual.pieces[piece], *expected[piece]);
        if (!result)
            return result << " in piece " << piece;
    }
    return testing::AssertionSuccess();
}

/// A batch of sources, one per maker.
struct BatchCase
{
    String name;
    std::vector<std::function<ColumnPtr(size_t rows)>> makes;
    /// The rows of each source. A source without an entry here gets 200 + 50 * index rows.
    std::vector<size_t> rows = {};
    bool exact = true;
};

/// Runs each case in the given order and in the reverse order, with random ids.
template <typename Id>
void expectSameAsPerSource(const std::vector<BatchCase> & cases, size_t num_pieces, bool allow_combining = false)
{
    pcg64 rng(selector_seed);
    for (const auto & batch_case : cases)
    {
        for (const bool reversed : {false, true})
        {
            auto makes = batch_case.makes;
            auto rows = batch_case.rows;
            for (size_t index = rows.size(); index < makes.size(); ++index)
                rows.push_back(200 + 50 * index);
            if (reversed)
            {
                std::ranges::reverse(makes);
                std::ranges::reverse(rows);
            }

            Batch<Id> batch;
            for (size_t index = 0; index < makes.size(); ++index)
                batch.add(makes[index](rows[index]), randomIds<Id>(rng, rows[index], num_pieces));
            EXPECT_TRUE(sameAsPerSource(batch, num_pieces, batch_case.exact, allow_combining)) << batch_case.name << (reversed ? ", in reverse order" : "");
        }
    }
}

}

template <typename Id>
class ColumnScatterByIdType : public ::testing::Test
{
};

using IdTypes = ::testing::Types<UInt64, UInt16>;
TYPED_TEST_SUITE(ColumnScatterByIdType, IdTypes);

/// Every piece must equal its oracle in values and in representation. A piece that loses the representation of its
/// source, such as `Sparse`, fails. An override may pass its counts to a nested column that has another selector. The
/// sum check catches that, or the recount in Debug and sanitizer builds.
TYPED_TEST(ColumnScatterByIdType, KeepsRepresentation)
{
    const auto & specs = sourceSpecs();
    std::vector<std::map<size_t, ColumnPtr>> sources(specs.size());

    for (const auto & selector_case : selectorCases<TypeParam>())
    {
        const auto & selector = selector_case.selector;
        const PieceRows piece_rows(selector, selector_case.pieces);

        const auto counts = countColumnsSizeInSelector(selector_case.pieces, selector);
        ASSERT_EQ(counts.size(), piece_rows.size()) << selector_case.name;
        for (size_t piece = 0; piece < counts.size(); ++piece)
            ASSERT_EQ(counts[piece], piece_rows.of(piece).size()) << selector_case.name << ", piece " << piece;

        for (size_t spec_index = 0; spec_index < specs.size(); ++spec_index)
        {
            const auto & spec = specs[spec_index];
            if (!runsCase(spec, selector_case))
                continue;

            auto & source = sources[spec_index][selector.size()];
            if (!source)
            {
                source = spec.make(selector.size());
                ASSERT_EQ(source->size(), selector.size()) << spec.name;
                if (spec.on_path && selector.size() >= 3)
                {
                    ASSERT_TRUE(spec.on_path(*source))
                        << spec.name << " with " << selector.size() << " rows is not the case it is named after";
                }
            }

            const auto expected = expectedPieces(spec.oracle, *source, piece_rows);
            ASSERT_TRUE(samePieces(spec, *source, piece_rows, expected, source->scatter(selector_case.pieces, selector)))
                << spec.name << ", " << selector_case.name;
        }
    }

    expectThrowingColumns<TypeParam>();
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
/// A call that breaks a precondition must throw the error code of the check that catches it. Several of these
/// checks throw `LOGICAL_ERROR`, which aborts Debug and sanitizer builds. So the test runs in Release builds only.
TYPED_TEST(ColumnScatterByIdType, Misuse)
{
    using Selector = PaddedPODArray<TypeParam>;
    const size_t rows = 4;

    /// A selector one row shorter or one row longer than the column. Each column must throw the code of its own size check.
    for (const size_t selector_rows : {rows - 1, rows + 1})
    {
        Selector selector;
        for (size_t row = 0; row < selector_rows; ++row)
            selector.push_back(static_cast<TypeParam>(row % 2));
        const auto counts = countColumnsSizeInSelector(2, selector);

        const auto expect_size_error = [&](const IColumn & column, int code)
        {
            expectErrorCode(code, column.getFamilyName(), [&] { (void)column.scatter(2, selector); });
            expectErrorCode(code, column.getFamilyName(), [&] { (void)column.scatterWithCounts(selector, counts); });
        };
        expect_size_error(*makeNumbers(rows), ErrorCodes::LOGICAL_ERROR);
        expect_size_error(*makeConst(rows), ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
        expect_size_error(*makeFunctionWithCaptures(rows), ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
        expect_size_error(*ColumnTuple::create(rows), ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
        expect_size_error(*ColumnNothing::create(rows), ErrorCodes::SIZES_OF_COLUMNS_DOESNT_MATCH);
    }

    /// Ids 0, 1, 1, 0: the right counts are {2, 2}. `scatterWithCounts` checks the sum before any override runs.
    Selector selector;
    for (size_t row = 0; row < rows; ++row)
        selector.push_back(static_cast<TypeParam>((row + 1) / 2 % 2));
    const std::vector<std::vector<size_t>> wrong_counts = {
        {2, 2, 1}, /// one piece too many, and the sum is 5
        {2}, /// one piece short, and the sum is 2
        {2, 1}, /// the right number of pieces, one row short
    };
    for (const auto & counts : wrong_counts)
    {
        for (const auto & column : {makeNumbers(rows), makeConst(rows), ColumnPtr(ColumnTuple::create(Columns{makeNumbers(rows)}))})
            expectErrorCode(ErrorCodes::LOGICAL_ERROR, column->getFamilyName(), [&] { (void)column->scatterWithCounts(selector, counts); });
    }

    /// `countRowsPerPiece` must throw for an id equal to the piece count, also in the second span, with the histogram
    /// lanes (up to 2048 pieces) and without.
    for (const size_t num_pieces : {size_t{2}, size_t{256}, size_t{2048}, size_t{2049}})
    {
        Selector first;
        Selector second;
        for (size_t row = 0; row < 9; ++row)
            first.push_back(static_cast<TypeParam>(row % num_pieces));
        for (size_t row = 0; row < 6; ++row)
            second.push_back(static_cast<TypeParam>(row == 4 ? num_pieces : row % num_pieces));
        const std::array<std::span<const TypeParam>, 2> spans{
            std::span<const TypeParam>(first.data(), first.size()), std::span<const TypeParam>(second.data(), second.size())};
        expectErrorCode(
            ErrorCodes::LOGICAL_ERROR,
            "countRowsPerPiece with an id equal to the piece count",
            [&] { (void)ColumnsScatter::countRowsPerPiece<TypeParam>(spans, num_pieces); });
    }

    /// The batched `ColumnsScatter::scatter` checks its spans and counts before it scatters, for a kernel type and for
    /// a type without a kernel.
    const std::array<std::span<const TypeParam>, 1> ids{std::span<const TypeParam>(selector.data(), selector.size())};
    /// Ids 0, 1, 1: the counts {1, 2} match them, but the source has a fourth row.
    const std::array<std::span<const TypeParam>, 1> short_ids{std::span<const TypeParam>(selector.data(), rows - 1)};
    const std::vector<size_t> counts_of_short_ids = {1, 2};
    const std::vector<size_t> right_counts = {2, 2};
    const std::vector<size_t> counts_one_row_short = {2, 1};
    const auto numbers = makeNumbers(rows);
    const auto low_cardinality = makeSpecSource("LowCardinality(String) with UInt8 indexes", rows);
    for (const auto & column : {numbers, low_cardinality})
    {
        const std::array<const IColumn *, 1> one_source{column.get()};
        const std::array<const IColumn *, 2> two_sources{column.get(), column.get()};
        expectErrorCode(
            ErrorCodes::LOGICAL_ERROR,
            "batched scatter with counts one row short",
            [&] { (void)ColumnsScatter::scatter<TypeParam>(one_source, ids, counts_one_row_short); });
        expectErrorCode(
            ErrorCodes::LOGICAL_ERROR,
            "batched scatter with an id span shorter than its source",
            [&] { (void)ColumnsScatter::scatter<TypeParam>(one_source, short_ids, counts_of_short_ids); });
        expectErrorCode(
            ErrorCodes::LOGICAL_ERROR,
            "batched scatter with two sources and one id span",
            [&] { (void)ColumnsScatter::scatter<TypeParam>(two_sources, ids, right_counts); });
    }

    /// Sources of different types: another `TypeIndex`, another width of the same `TypeIndex`, another tuple size.
    const std::vector<std::pair<ColumnPtr, ColumnPtr>> different_types = {
        {numbers, randomFixed<ColumnUInt32>()(rows)},
        {randomFixed<ColumnFixedString>(size_t{4})(rows), randomFixed<ColumnFixedString>(size_t{8})(rows)},
        {tupleOf({randomFixed<ColumnUInt64>()})(rows), tupleOf({randomFixed<ColumnUInt64>(), randomFixed<ColumnUInt64>()})(rows)},
    };
    const std::array<std::span<const TypeParam>, 2> two_ids{ids[0], ids[0]};
    const std::vector<size_t> two_counts = {4, 4};
    for (const auto & [first, second] : different_types)
    {
        const std::array<const IColumn *, 2> sources{first.get(), second.get()};
        expectErrorCode(
            ErrorCodes::LOGICAL_ERROR,
            second->getFamilyName(),
            [&] { (void)ColumnsScatter::scatter<TypeParam>(sources, two_ids, two_counts); });
    }

    /// 16-bit ids address at most 65536 pieces.
    if constexpr (std::is_same_v<TypeParam, UInt16>)
    {
        expectErrorCode(
            ErrorCodes::LOGICAL_ERROR, "countColumnsSizeInSelector", [&] { (void)countColumnsSizeInSelector(65537, selector); });
        expectErrorCode(ErrorCodes::LOGICAL_ERROR, "UInt64", [&] { (void)makeNumbers(rows)->scatter(65537, selector); });
        expectErrorCode(ErrorCodes::LOGICAL_ERROR, "Const", [&] { (void)makeConst(rows)->scatter(65537, selector); });
        expectErrorCode(
            ErrorCodes::LOGICAL_ERROR, "countRowsPerPiece", [&] { (void)ColumnsScatter::countRowsPerPiece<UInt16>(ids, 65537); });

        /// 65537 counts that the caller made and that sum to the number of ids. Only the bound on the piece count
        /// catches them, for a kernel type and for a type without a kernel.
        std::vector<size_t> too_many_counts(65537, 0);
        too_many_counts[0] = 2;
        too_many_counts[1] = 2;
        for (const auto & column : {numbers, low_cardinality})
        {
            const std::array<const IColumn *, 1> one_source{column.get()};
            expectErrorCode(
                ErrorCodes::LOGICAL_ERROR,
                column->getFamilyName(),
                [&] { (void)ColumnsScatter::scatter<UInt16>(one_source, ids, too_many_counts); });
        }
    }
}
#endif

/// 65536 pieces is the most that 16-bit ids address. Five calls must accept it: `countColumnsSizeInSelector`,
/// `countRowsPerPiece`, `scatter`, `scatterWithCounts` and the batched `ColumnsScatter::scatter`. The rows with id 65535
/// must reach the last piece in row order. An off-by-one in the bound check would reject the count or lose the last piece.
TEST(ColumnScatter, SmallSelectorBoundary)
{
    constexpr size_t num_pieces = 65536;
    constexpr size_t rows = 1000;

    /// Every fourth row goes to id 65535. Each other row goes to the piece with its own row number.
    IColumn::SmallSelector selector;
    for (size_t row = 0; row < rows; ++row)
        selector.push_back(static_cast<UInt16>(row % 4 == 3 ? num_pieces - 1 : row));

    const auto counts = countColumnsSizeInSelector(num_pieces, selector);
    ASSERT_EQ(counts.size(), num_pieces);
    for (size_t piece = 0; piece < num_pieces; ++piece)
    {
        size_t expected = 0;
        if (piece == num_pieces - 1)
            expected = rows / 4;
        else if (piece < rows && piece % 4 != 3)
            expected = 1;
        ASSERT_EQ(counts[piece], expected) << "piece " << piece;
    }

    const std::array<std::span<const UInt16>, 1> ids{std::span<const UInt16>(selector.data(), selector.size())};
    ASSERT_TRUE(std::ranges::equal(ColumnsScatter::countRowsPerPiece<UInt16>(ids, num_pieces), counts));

    const auto numbers = makeNumbers(rows);
    const auto strings = makeStrings(rows);
    for (const auto & column : {numbers, strings})
    {
        const std::array<const IColumn *, 1> one_source{column.get()};
        for (const std::string_view entry : {"scatter", "scatterWithCounts", "batched ColumnsScatter::scatter"})
        {
            VectorWithMemoryTracking<MutableColumnPtr> pieces;
            if (entry == "scatter")
                pieces = column->scatter(num_pieces, selector);
            else if (entry == "scatterWithCounts")
                pieces = column->scatterWithCounts(selector, counts);
            else
                pieces = ColumnsScatter::scatter<UInt16>(one_source, ids, counts);
            const String where = fmt::format("{}, {}", column->getName(), entry);
            ASSERT_EQ(pieces.size(), num_pieces) << where;
            for (size_t piece = 0; piece < num_pieces; ++piece)
                ASSERT_EQ(pieces[piece]->size(), counts[piece]) << where << ", piece " << piece;

            const auto & last = *pieces.back();
            for (size_t row = 0; row < last.size(); ++row)
                ASSERT_EQ(last[row], (*column)[row * 4 + 3]) << where << ", row " << row << " of the last piece";
            for (size_t row = 0; row < rows; ++row)
            {
                if (row % 4 != 3)
                    ASSERT_EQ((*pieces[row])[0], (*column)[row]) << where << ", piece " << row;
            }
        }
    }
}

/// `IColumn::scatter` calls `ColumnsScatter::scatter` for every type that reaches a kernel. Each piece must equal
/// `insertFromPiece`, byte for byte in every part and with the same structure, and have the exact size that
/// `exactPiece` checks. A dropped, duplicated or reordered row, a wrong offset rebase, a wrong null map or a wrong
/// element range changes the bytes. A piece sized from wrong counts is too large or too small. A write past a piece
/// is an ASan report.
TYPED_TEST(ColumnScatterByIdType, MatchesInsertFromForEveryKernelType)
{
    const auto & specs = kernelSpecs();
    std::vector<std::map<size_t, ColumnPtr>> sources(specs.size());

    for (const auto & selector_case : selectorCases<TypeParam>())
    {
        const auto & selector = selector_case.selector;
        const PieceRows piece_rows(selector, selector_case.pieces);

        for (size_t spec_index = 0; spec_index < specs.size(); ++spec_index)
        {
            const auto & spec = specs[spec_index];
            if (!runsCase(spec, selector_case))
                continue;

            auto & source = sources[spec_index][selector.size()];
            if (!source)
                source = spec.make(selector.size());

            const ColumnPtr empty_piece = source->cloneEmpty();
            const auto pieces = source->scatter(selector_case.pieces, selector);
            ASSERT_EQ(pieces.size(), selector_case.pieces) << spec.name << ", " << selector_case.name;
            for (size_t piece = 0; piece < pieces.size(); ++piece)
            {
                const auto rows = piece_rows.of(piece);
                ASSERT_EQ(pieces[piece]->size(), rows.size()) << spec.name << ", " << selector_case.name << ", piece " << piece;

                /// One oracle piece at a time: an `insertFrom` piece of one row takes 4 KiB, and a case has up to 65537 pieces.
                const ColumnPtr oracle = rows.empty() ? empty_piece : ColumnPtr(insertFromPiece(*source, rows));
                const auto same = comparedInFull(piece, pieces.size(), rows.size())
                    ? sameColumn(*oracle, *pieces[piece], piece < 1024 || piece + 1 == pieces.size())
                    : sameRepresentation(*source, *pieces[piece]);
                ASSERT_TRUE(same) << spec.name << ", " << selector_case.name << ", piece " << piece;
                ASSERT_TRUE(exactPiece(*pieces[piece], *oracle)) << spec.name << ", " << selector_case.name << ", piece " << piece;
            }
        }
    }
}

/// `hasKernel` accepts exactly the types that its contract lists. A type that falls back to the `insertFrom` loop
/// gives the same pieces, so only this test catches it.
TEST(ColumnScatter, HasKernel)
{
    constexpr size_t rows = 5;
    for (const auto & spec : kernelSpecs())
    {
        const auto column = spec.make(rows);
        if (const auto * map = typeid_cast<const ColumnMap *>(column.get()))
        {
            /// A `Map` keeps its own `scatter`, which passes the counts to its nested `Array`.
            EXPECT_FALSE(ColumnsScatter::hasKernel(*column)) << spec.name;
            EXPECT_TRUE(ColumnsScatter::hasKernel(map->getNestedColumn())) << spec.name;
        }
        else
        {
            EXPECT_TRUE(ColumnsScatter::hasKernel(*column)) << spec.name;
        }
    }

    for (const auto & spec : sourceSpecs())
        EXPECT_FALSE(ColumnsScatter::hasKernel(*spec.make(rows))) << spec.name;
    for (const auto & spec : otherSpecs())
        EXPECT_FALSE(ColumnsScatter::hasKernel(*spec.make(rows))) << spec.name;
    for (const auto & [name, column] : makeRejectedColumns(rows))
        EXPECT_FALSE(ColumnsScatter::hasKernel(*column)) << name;
}

/// With one source, the batched `ColumnsScatter::scatter` returns what `IColumn::scatter` returns for every type and
/// representation: the same bytes and structure, the same wrappers, the same `LowCardinality` dictionaries, index
/// types and sharing, and views on the same aggregate states. A one-source batch that normalized its source, or took
/// a kernel that `hasKernel` rejects, would change them.
TYPED_TEST(ColumnScatterByIdType, BatchedSingleSourceMatchesScatter)
{
    const auto & specs = allSpecs();
    std::vector<std::map<size_t, ColumnPtr>> sources(specs.size());

    for (const auto & selector_case : selectorCases<TypeParam>())
    {
        const auto & selector = selector_case.selector;
        const std::array<std::span<const TypeParam>, 1> ids{std::span<const TypeParam>(selector.data(), selector.size())};
        const auto counts = ColumnsScatter::countRowsPerPiece<TypeParam>(ids, selector_case.pieces);
        ASSERT_TRUE(std::ranges::equal(counts, countColumnsSizeInSelector(selector_case.pieces, selector))) << selector_case.name;

        /// With one source, `IColumn::scatter` and the batched `ColumnsScatter::scatter` make the same call with the
        /// same arguments, and the type alone picks the call. So the short set is enough.
        if (!selector_case.in_short_set)
            continue;

        const PieceRows piece_rows(selector, selector_case.pieces);
        for (size_t spec_index = 0; spec_index < specs.size(); ++spec_index)
        {
            const auto & spec = specs[spec_index];
            auto & source = sources[spec_index][selector.size()];
            if (!source)
                source = spec.make(selector.size());

            const std::array<const IColumn *, 1> one_source{source.get()};
            const auto expected = toColumns(source->scatter(selector_case.pieces, selector));
            ASSERT_TRUE(samePieces(spec, *source, piece_rows, expected, ColumnsScatter::scatter<TypeParam>(one_source, ids, counts)))
                << spec.name << ", " << selector_case.name;
        }
    }
}

/// Three sources of each kernel type, the middle one without rows. Each piece must hold the rows of every source in
/// source order, as `perSourcePieces` builds them, and have its exact size.
TYPED_TEST(ColumnScatterByIdType, BatchedMatchesPerSourceScatter)
{
    const auto & specs = kernelSpecs();
    std::vector<std::map<size_t, ColumnPtr>> sources(specs.size());

    for (const auto & selector_case : selectorCases<TypeParam>())
    {
        /// Apart from the store mode, the batched `ColumnsScatter::scatter` and the kernels branch on the type, the width
        /// and empty pieces, never on the row or piece count, so the short set reaches every branch. `BatchedWriteCombining`
        /// covers the store mode. `SmallSelectorBoundary` runs 65536 pieces, and `HistogramLanes` counts rows for more
        /// pieces than the lanes take.
        if (!selector_case.in_short_set)
            continue;

        const auto & selector = selector_case.selector;
        const size_t num_rows = selector.size();
        const size_t first_rows = num_rows / 3;

        for (size_t spec_index = 0; spec_index < specs.size(); ++spec_index)
        {
            const auto & spec = specs[spec_index];
            auto & source = sources[spec_index][num_rows];
            if (!source)
                source = spec.make(num_rows);

            Batch<TypeParam> batch;
            batch.add(source->cut(0, first_rows), sliceOf(selector, 0, first_rows));
            batch.add(source->cut(first_rows, 0), {});
            batch.add(source->cut(first_rows, num_rows - first_rows), sliceOf(selector, first_rows, num_rows));
            ASSERT_TRUE(sameAsPerSource(batch, selector_case.pieces, /*exact=*/true)) << spec.name << ", " << selector_case.name;
        }
    }
}

/// When all sources are `Const` with byte-equal values, each piece is `cloneResized` of the first source. It shares
/// the data of that source and holds exactly its own rows, also when every row goes to one piece. Different values
/// are materialized. So are `+0.0` and `-0.0`, which compare equal but differ in bytes, and they keep their bytes.
TYPED_TEST(ColumnScatterByIdType, BatchedConstPath)
{
    constexpr size_t num_pieces = 4;
    pcg64 rng(selector_seed);

    const auto expect_compact = [](const Batch<TypeParam> & batch, const BatchResult & result, UInt64 value)
    {
        const auto & first = assert_cast<const ColumnConst &>(*batch.sources.front());
        ASSERT_EQ(result.pieces.size(), result.counts.size());
        for (size_t piece = 0; piece < result.pieces.size(); ++piece)
        {
            const auto * piece_const = typeid_cast<const ColumnConst *>(result.pieces[piece].get());
            ASSERT_NE(piece_const, nullptr) << "piece " << piece << " is " << result.pieces[piece]->dumpStructure();
            ASSERT_EQ(piece_const->size(), result.counts[piece]) << "piece " << piece;
            ASSERT_EQ(piece_const->getDataColumnPtr().get(), first.getDataColumnPtr().get()) << "piece " << piece;
            ASSERT_EQ(piece_const->getDataColumn().getUInt(0), value) << "piece " << piece;
        }
    };

    {
        Batch<TypeParam> batch;
        for (const size_t rows : {size_t{100}, size_t{0}, size_t{60}})
            batch.add(makeConstOf<ColumnUInt64>(7, rows), randomIds<TypeParam>(rng, rows, num_pieces));
        expect_compact(batch, scatterBatch(batch, num_pieces), 7);
    }

    {
        Batch<TypeParam> batch;
        for (const size_t rows : {size_t{2'000'000}, size_t{1'000'000}})
        {
            PaddedPODArray<TypeParam> ids;
            ids.resize_fill(rows, 0);
            batch.add(makeConstOf<ColumnUInt64>(12345, rows), std::move(ids));
        }
        const auto result = scatterBatch(batch, num_pieces);
        ASSERT_EQ(result.counts.front(), size_t{3'000'000});
        expect_compact(batch, result, 12345);
    }

    {
        Batch<TypeParam> batch;
        batch.add(makeConstOf<ColumnFloat64>(0.0, 40), randomIds<TypeParam>(rng, 40, num_pieces));
        batch.add(makeConstOf<ColumnFloat64>(-0.0, 30), randomIds<TypeParam>(rng, 30, num_pieces));
        ASSERT_TRUE(sameAsPerSource(batch, num_pieces, /*exact=*/true));

        const auto result = scatterBatch(batch, num_pieces);
        size_t negative_zeros = 0;
        for (const auto & piece : result.pieces)
        {
            const auto * values = typeid_cast<const ColumnFloat64 *>(piece.get());
            ASSERT_NE(values, nullptr) << piece->dumpStructure();
            for (const Float64 value : values->getData())
            {
                if (value == 0.0 && std::signbit(value))
                    ++negative_zeros;
            }
        }
        ASSERT_EQ(negative_zeros, size_t{30});
    }

    {
        Batch<TypeParam> batch;
        batch.add(makeConstOf<ColumnUInt64>(5, 250), randomIds<TypeParam>(rng, 250, num_pieces));
        batch.add(makeConstOf<ColumnUInt64>(9, 250), randomIds<TypeParam>(rng, 250, num_pieces));
        ASSERT_TRUE(sameAsPerSource(batch, num_pieces, /*exact=*/true));
    }
}

/// A `Function` cannot be serialized, so `Const(Function)` values cannot be compared byte for byte. With one source
/// the pieces are those of `ColumnConst::scatter`. Several sources are materialized, without an exception.
TYPED_TEST(ColumnScatterByIdType, BatchedConstFunction)
{
    constexpr size_t num_pieces = 4;
    pcg64 rng(selector_seed);

    {
        Batch<TypeParam> batch;
        batch.add(makeConstFunction(300), randomIds<TypeParam>(rng, 300, num_pieces));
        const auto expected = batch.sources.front()->scatter(num_pieces, batch.ids.front());
        const auto result = scatterBatch(batch, num_pieces);
        ASSERT_EQ(result.pieces.size(), num_pieces);
        for (size_t piece = 0; piece < num_pieces; ++piece)
            ASSERT_TRUE(sameColumn(*expected[piece], *result.pieces[piece], true)) << "piece " << piece;
    }

    expectSameAsPerSource<TypeParam>(
        {{.name = "Const(Function)", .makes = {makeConstFunction, makeConstFunction, makeConstFunction}, .exact = false}}, num_pieces);
}

/// Sources of one type in different representations give the pieces of `perSourcePieces`, in either order. `Const`,
/// `Sparse` and `Replicated` are materialized, also inside a `Tuple`, an `Array` or a `Map`. `LowCardinality` stays.
TYPED_TEST(ColumnScatterByIdType, BatchedMixedRepresentations)
{
    const auto sparse = [](size_t rows) { return makeSparse(rows, 0); };
    const auto tuple_of_numbers = [](size_t rows) -> ColumnPtr { return ColumnTuple::create(Columns{makeNumbers(rows)}); };
    const auto tuple_of_sparse = [](size_t rows) -> ColumnPtr { return ColumnTuple::create(Columns{makeSparse(rows, 0)}); };
    const auto array_of_numbers = [](size_t rows) -> ColumnPtr { return ColumnArray::create(makeNumbers(rows), makeOffsets(rows)); };
    const auto array_of_sparse = [](size_t rows) -> ColumnPtr { return ColumnArray::create(makeSparse(rows, 0), makeOffsets(rows)); };
    const auto map_of_numbers = [](size_t rows) -> ColumnPtr
    { return ColumnMap::create(ColumnArray::create(ColumnTuple::create(Columns{makeNumbers(rows), makeNumbers(rows)}), makeOffsets(rows))); };
    const auto map_of_sparse = [](size_t rows) -> ColumnPtr
    {
        return ColumnMap::create(
            ColumnArray::create(ColumnTuple::create(Columns{makeNumbers(rows), makeSparse(rows, 0)}), makeOffsets(rows)));
    };
    const auto low_cardinality = [](size_t rows) { return makeSpecSource("LowCardinality(String) with UInt8 indexes", rows); };
    const auto const_low_cardinality = [](size_t rows) -> ColumnPtr
    { return ColumnConst::create(makeSpecSource("LowCardinality(String) with UInt8 indexes", 1), rows); };
    const auto const_string = [](size_t rows) { return makeConstString("konst", rows); };

    expectSameAsPerSource<TypeParam>(
        {
            {.name = "Const(UInt64) and UInt64", .makes = {makeConst, makeNumbers}},
            {.name = "Sparse(UInt64) and UInt64", .makes = {sparse, makeNumbers}},
            {.name = "Replicated(String) and String", .makes = {makeReplicated, makeStrings}},
            {.name = "String, Const(String) and String", .makes = {makeStrings, const_string, makeStrings}},
            {.name = "Nullable(String), Const(Nullable(String)) and Nullable(String)",
             .makes = {nullableOf(makeMixedStrings), makeConstNullableString, nullableOf(makeMixedStrings)}},
            {.name = "Tuple(UInt64) and Tuple(Sparse(UInt64))", .makes = {tuple_of_numbers, tuple_of_sparse}},
            {.name = "Array(UInt64) and Array(Sparse(UInt64))", .makes = {array_of_numbers, array_of_sparse}},
            {.name = "Map(UInt64, UInt64) and Map(UInt64, Sparse(UInt64))", .makes = {map_of_numbers, map_of_sparse}},
            {.name = "LowCardinality(String), Const(LowCardinality(String)) and LowCardinality(String)",
             .makes = {low_cardinality, const_low_cardinality, low_cardinality},
             .exact = false},
        },
        5);
}

/// Several `LowCardinality` sources with different dictionaries and index widths give the pieces of
/// `perSourcePieces`, at the top level and inside a `Tuple`, an `Array`, a `Map` and a `Nullable(Tuple)`. A structure
/// check that compared the index widths of nested `LowCardinality` columns would abort Debug builds here.
TYPED_TEST(ColumnScatterByIdType, BatchedLowCardinality)
{
    const auto narrow = [](size_t rows) { return makeSpecSource("LowCardinality(String) with UInt8 indexes", rows); };
    const auto wide = [](size_t rows) { return makeSpecSource("LowCardinality(String) with UInt16 indexes", rows); };
    const auto tuple_of_narrow = [](size_t rows) -> ColumnPtr
    { return ColumnTuple::create(Columns{makeSpecSource("LowCardinality(String) with UInt8 indexes", rows), makeNumbers(rows)}); };
    const auto tuple_of_wide = [](size_t rows) -> ColumnPtr
    { return ColumnTuple::create(Columns{makeSpecSource("LowCardinality(String) with UInt16 indexes", rows), makeNumbers(rows)}); };
    const auto array_of_low_cardinality = [](size_t rows) { return makeSpecSource("Array(LowCardinality(String))", rows); };
    const auto map_of_low_cardinality = [](size_t rows) { return makeSpecSource("Map(LowCardinality(String), UInt64)", rows); };
    const auto nullable_tuple = [](size_t rows) { return makeSpecSource("Nullable(Tuple(LowCardinality(String), UInt64))", rows); };

    expectSameAsPerSource<TypeParam>(
        {
            {.name = "LowCardinality(String)", .makes = {narrow, wide, narrow}, .exact = false},
            {.name = "Tuple(LowCardinality(String), UInt64)", .makes = {tuple_of_narrow, tuple_of_wide}, .exact = false},
            {.name = "Array(LowCardinality(String))", .makes = {array_of_low_cardinality, array_of_low_cardinality}, .exact = false},
            {.name = "Map(LowCardinality(String), UInt64)", .makes = {map_of_low_cardinality, map_of_low_cardinality}, .exact = false},
            {.name = "Nullable(Tuple(LowCardinality(String), UInt64))", .makes = {nullable_tuple, nullable_tuple}, .exact = false},
        },
        4);
}

/// A source without rows adds nothing, wherever it is in the batch. A batch without rows gives pieces that hold
/// what `cloneEmpty` holds.
TYPED_TEST(ColumnScatterByIdType, BatchedEmptySources)
{
    const ColumnMaker nullable_strings = nullableOf(makeMixedStrings);
    const ColumnMaker arrays_of_numbers = arrayOf(randomFixed<ColumnUInt64>());
    const auto low_cardinality = [](size_t rows) { return makeSpecSource("LowCardinality(String) with UInt8 indexes", rows); };
    const auto variant = [](size_t rows) { return makeSpecSource("Variant with several variants and NULLs", rows); };

    expectSameAsPerSource<TypeParam>(
        {
            {.name = "UInt64", .makes = {makeNumbers, makeNumbers, makeNumbers}, .rows = {100, 0, 50}},
            {.name = "UInt64 without rows", .makes = {makeNumbers, makeNumbers}, .rows = {0, 0}},
            {.name = "String", .makes = {makeStrings, makeStrings, makeStrings}, .rows = {0, 30, 0}},
            {.name = "Nullable(String)", .makes = {nullable_strings, nullable_strings}, .rows = {40, 0}},
            {.name = "Array(UInt64)", .makes = {arrays_of_numbers, arrays_of_numbers}, .rows = {0, 25}},
            {.name = "LowCardinality(String)", .makes = {low_cardinality, low_cardinality}, .rows = {0, 20}, .exact = false},
            {.name = "Variant(String, UInt64)", .makes = {variant, variant}, .rows = {30, 0}, .exact = false},
        },
        4);

    expectSameAsPerSource<TypeParam>({{.name = "String in 512 pieces", .makes = {makeStrings, makeStrings}, .rows = {250, 250}}}, 512);
}

/// The histogram lanes split the chain of increments in four. Around the limits of the lanes and at every tail
/// length, the counts must equal a plain count, from one call and from two calls that carry the lanes over.
TEST(ColumnScatter, HistogramLanes)
{
    pcg64 rng(selector_seed);
    for (const size_t fanout : std::to_array<size_t>({2, 255, 256, 2048, 2049}))
    {
        for (const size_t num_rows : std::to_array<size_t>({0, 1, 2, 3, 1000, 1001, 1002, 1003}))
        {
            auto pids = randomIds<UInt16>(rng, num_rows, fanout);
            if (num_rows > 0)
            {
                pids.front() = static_cast<UInt16>(fanout - 1);
                pids.back() = 0;
            }
            std::vector<size_t> expected(fanout, 0);
            for (const auto pid : pids)
                ++expected[pid];

            PaddedPODArray<UInt64> wide_pids;
            for (const auto pid : pids)
                wide_pids.push_back(pid);

            for (const size_t split : {num_rows, num_rows / 3})
            {
                const String where = fmt::format("fanout {}, rows {}, split at {}", fanout, num_rows, split);
                const size_t rest = num_rows - split;

                PaddedPODArray<UInt64> with_lanes;
                with_lanes.resize_fill(fanout, 0);
                PaddedPODArray<UInt64> lanes;
                lanes.resize_fill(4 * fanout, 0);
                ColumnsScatter::histogramPidChunk(pids.data(), split, with_lanes.data(), lanes.data(), fanout);
                ColumnsScatter::histogramPidChunk(pids.data() + split, rest, with_lanes.data(), lanes.data(), fanout);
                ColumnsScatter::reduceHistogramLanes(with_lanes.data(), lanes.data(), fanout);
                ASSERT_TRUE(std::ranges::equal(with_lanes, expected)) << where << ", histogramPidChunk with lanes";

                PaddedPODArray<UInt64> without_lanes;
                without_lanes.resize_fill(fanout, 0);
                ColumnsScatter::histogramPidChunk(pids.data(), split, without_lanes.data(), nullptr, fanout);
                ColumnsScatter::histogramPidChunk(pids.data() + split, rest, without_lanes.data(), nullptr, fanout);
                ASSERT_TRUE(std::ranges::equal(without_lanes, expected)) << where << ", histogramPidChunk without lanes";

                const std::array<std::span<const UInt16>, 2> small_ids{
                    std::span<const UInt16>(pids.data(), split), std::span<const UInt16>(pids.data() + split, rest)};
                ASSERT_TRUE(std::ranges::equal(ColumnsScatter::countRowsPerPiece<UInt16>(small_ids, fanout), expected))
                    << where << ", countRowsPerPiece with UInt16 ids";

                const std::array<std::span<const UInt64>, 2> large_ids{
                    std::span<const UInt64>(wide_pids.data(), split), std::span<const UInt64>(wide_pids.data() + split, rest)};
                ASSERT_TRUE(std::ranges::equal(ColumnsScatter::countRowsPerPiece<UInt64>(large_ids, fanout), expected))
                    << where << ", countRowsPerPiece with UInt64 ids";
            }
        }
    }
}

/// The pass widths that the contract of `computePassBits` gives, such as 8 + 7 for 15 bits under a 10-bit cap. A
/// greedy split gives 10 + 5 and fails the table.
TEST(ColumnScatter, ComputePassBits)
{
    struct Case
    {
        size_t fanout = 0;
        size_t max_fanout_per_pass = 0;
        std::vector<size_t> passes;
    };
    const std::vector<Case> cases = {
        {0, 1 << 8, {}},
        {0, 1 << 10, {}},
        {1, 1 << 8, {}},
        {1, 1 << 10, {}},
        {2, 1 << 8, {1}},
        {2, 1 << 10, {1}},
        {1 << 10, 1 << 8, {5, 5}},
        {1 << 10, 1 << 10, {10}},
        {1 << 15, 1 << 8, {8, 7}},
        {1 << 15, 1 << 10, {8, 7}},
    };
    for (const auto & test_case : cases)
    {
        EXPECT_TRUE(std::ranges::equal(ColumnsScatter::computePassBits(test_case.fanout, test_case.max_fanout_per_pass), test_case.passes))
            << "fanout " << test_case.fanout << ", at most " << test_case.max_fanout_per_pass << " per pass";
    }
}

/// The chunk kernels in the order a caller uses them: a histogram over two uneven chunks with and without the lanes,
/// one exact allocation per piece, cursors seeded once per column, then both chunks scattered and drained. One scratch
/// serves columns of widths 8, 2, 4 and 32, in the direct mode and, for the widths it supports, in the combined mode.
/// The histogram must equal a plain count, and every piece must equal `insertFromPiece`.
TEST(ColumnScatter, ChunkKernelsCompose)
{
    constexpr size_t num_rows = 10000;
    constexpr size_t first_chunk_rows = 3001;
    const std::array<std::pair<size_t, size_t>, 2> chunks{{{0, first_chunk_rows}, {first_chunk_rows, num_rows - first_chunk_rows}}};
    const std::array<ColumnPtr, 4> group{
        randomFixed<ColumnUInt64>()(num_rows),
        randomFixed<ColumnUInt16>()(num_rows),
        randomFixed<ColumnUInt32>()(num_rows),
        randomFixed<ColumnUInt256>()(num_rows)};

    pcg64 rng(selector_seed);
    for (const size_t fanout : {size_t{64}, size_t{1024}})
    {
        auto pids = randomIds<UInt16>(rng, num_rows, fanout);
        pids.back() = static_cast<UInt16>(fanout - 1);
        const PieceRows piece_rows(pids, fanout);

        PaddedPODArray<UInt64> hist;
        hist.resize_fill(fanout, 0);
        PaddedPODArray<UInt64> lanes;
        lanes.resize_fill(4 * fanout, 0);
        PaddedPODArray<UInt64> plain;
        plain.resize_fill(fanout, 0);
        for (const auto & [offset, length] : chunks)
        {
            ColumnsScatter::histogramPidChunk(pids.data() + offset, length, hist.data(), lanes.data(), fanout);
            ColumnsScatter::histogramPidChunk(pids.data() + offset, length, plain.data(), nullptr, fanout);
        }
        ColumnsScatter::reduceHistogramLanes(hist.data(), lanes.data(), fanout);
        for (size_t piece = 0; piece < fanout; ++piece)
        {
            ASSERT_EQ(hist[piece], piece_rows.of(piece).size()) << "fanout " << fanout << ", piece " << piece << ", with lanes";
            ASSERT_EQ(plain[piece], piece_rows.of(piece).size()) << "fanout " << fanout << ", piece " << piece << ", without lanes";
        }

        for (const auto scratch_mode : {ColumnsScatter::StoreMode::Direct, ColumnsScatter::StoreMode::Combined})
        {
            ColumnsScatter::ScatterScratch scratch;
            scratch.init(fanout, scratch_mode);
            for (const auto & column : group)
            {
                const size_t width = column->sizeOfValueIfFixed();
                const auto mode = width <= 16 ? scratch_mode : ColumnsScatter::StoreMode::Direct;
                scratch.setStoreMode(mode);
                MutableColumns pieces(fanout);
                for (size_t piece = 0; piece < fanout; ++piece)
                {
                    auto [allocated, cursor] = ColumnsScatter::allocateUninitializedFixed(*column, hist[piece]);
                    ASSERT_EQ(allocated->size(), hist[piece]) << column->getName() << ", piece " << piece;
                    ASSERT_EQ(cursor.size(), hist[piece] * width) << column->getName() << ", piece " << piece;
                    scratch.seed(piece, cursor.data());
                    pieces[piece] = std::move(allocated);
                }

                const char * data = column->getRawData().data();
                for (const auto & [offset, length] : chunks)
                    ColumnsScatter::scatterPidChunk(width, pids.data() + offset, data + offset * width, length, mode, scratch);
                scratch.drain();

                for (size_t piece = 0; piece < fanout; ++piece)
                {
                    ASSERT_TRUE(sameColumn(*insertFromPiece(*column, piece_rows.of(piece)), *pieces[piece], true))
                        << column->getName() << ", fanout " << fanout << ", piece " << piece << ", store mode "
                        << static_cast<UInt32>(mode);
                }
            }
        }
    }
}

/// The limits of `chooseStoreMode`: the widths 1, 2, 4, 8 and 16, from 256 to 65536 pieces, and at least 2 lines of rows
/// per piece on average.
TEST(ColumnScatter, StoreModeTable)
{
    using ColumnsScatter::StoreMode;
    const auto combined = [](size_t width, size_t pieces, size_t rows)
    { return ColumnsScatter::chooseStoreMode(width, pieces, rows, true) == StoreMode::Combined; };

    /// Without the hint, no shape combines.
    for (const size_t width : {1, 2, 4, 8, 16})
        EXPECT_EQ(ColumnsScatter::chooseStoreMode(width, 1024, size_t{1} << 30, false), StoreMode::Direct) << "width " << width;

    if (!ColumnsScatter::COMBINING_ENABLED)
    {
        for (const size_t width : {1, 2, 4, 8, 16})
            EXPECT_FALSE(combined(width, 1024, size_t{1} << 30)) << "width " << width;
        return;
    }

    for (const size_t width : {1, 2, 4, 8, 16})
    {
        const size_t line_rows = ColumnsScatter::LINE_BYTES / width;
        const size_t enough = 256 * ColumnsScatter::COMBINING_MIN_LINES_PER_PIECE * line_rows;
        EXPECT_TRUE(combined(width, 256, enough)) << "width " << width;
        EXPECT_FALSE(combined(width, 256, enough - 1)) << "width " << width << ", one row too few";
        EXPECT_FALSE(combined(width, 255, 1000 * enough)) << "width " << width << ", 255 pieces";
        EXPECT_TRUE(combined(width, 65536, 256 * enough)) << "width " << width << ", 65536 pieces";
        EXPECT_FALSE(combined(width, 65537, 1000 * 256 * enough)) << "width " << width << ", 65537 pieces";
        EXPECT_FALSE(combined(width, 1024, 0)) << "width " << width << ", no rows";
    }
    for (const size_t width : {0, 3, 5, 12, 24, 32, 64, 100})
        EXPECT_FALSE(combined(width, 1024, size_t{1} << 30)) << "width " << width;
}

namespace
{

/// The piece ids of the cases that stress the staging lines: every piece gets rows, a few pieces get them, one piece
/// takes 90% of them, one piece takes all of them, and the pieces take them in turn.
template <typename Id>
PaddedPODArray<Id> combiningIds(size_t kind, size_t rows, size_t pieces)
{
    pcg64 rng(selector_seed + kind);
    PaddedPODArray<Id> ids;
    ids.reserve_exact(rows);
    for (size_t row = 0; row < rows; ++row)
    {
        size_t piece = 0;
        switch (kind)
        {
            case 0: piece = rng() % pieces; break;
            case 1: piece = rng() % 4 == 0 ? pieces - 1 : rng() % 3; break;
            case 2: piece = rng() % 10 != 0 ? 7 : rng() % pieces; break;
            case 3: piece = 5; break;
            default: piece = row % pieces; break;
        }
        ids.push_back(static_cast<Id>(piece));
    }
    return ids;
}

struct CombiningShape
{
    String name;
    ColumnMaker make;
    /// The widths of the parts that are written row by row.
    std::vector<size_t> widths;
    std::vector<size_t> pieces;
};

}

/// For the shapes where `chooseStoreMode` turns write-combining on, the batched `scatter` with the hint must give the
/// pieces of the `insertFrom` loop, with the exact sizes. Where `COMBINING_ENABLED` is false, the shapes run direct. The shapes have the 5 widths, the null map and the values of a `Nullable`, and the
/// elements of a `Tuple`. The elements of an `Array` are copied by ranges, so they stay direct.
TYPED_TEST(ColumnScatterByIdType, WriteCombiningMatchesInsertFrom)
{
    using Id = TypeParam;
    const std::vector<CombiningShape> shapes = {
        {"UInt8", randomFixed<ColumnUInt8>(), {1}, {256, 1024}},
        {"UInt16", randomFixed<ColumnUInt16>(), {2}, {256, 1024}},
        {"UInt32", randomFixed<ColumnUInt32>(), {4}, {256, 1024}},
        {"UInt64", randomFixed<ColumnUInt64>(), {8}, {256, 1024, 65536}},
        {"FixedString(16)", randomFixed<ColumnFixedString>(size_t{16}), {16}, {256, 1024}},
        {"Nullable(UInt64)", nullableOf(randomFixed<ColumnUInt64>()), {1, 8}, {256, 1024}},
        {"Nullable(Tuple(UInt16, UInt64))",
         nullableOf(tupleOf({randomFixed<ColumnUInt16>(), randomFixed<ColumnUInt64>()})),
         {1, 2, 8},
         {256}},
        {"Array(UInt64)", arrayOf(randomFixed<ColumnUInt64>()), {}, {256}},
    };

    for (const auto & shape : shapes)
    {
        for (const size_t pieces : shape.pieces)
        {
            /// The narrowest part needs the most rows.
            const size_t narrowest = shape.widths.empty() ? 8 : *std::ranges::min_element(shape.widths);
            const size_t rows = pieces * ColumnsScatter::COMBINING_MIN_LINES_PER_PIECE * (ColumnsScatter::LINE_BYTES / narrowest) + 777;
            for (const size_t width : shape.widths)
                ASSERT_EQ(ColumnsScatter::chooseStoreMode(width, pieces, rows, true) == ColumnsScatter::StoreMode::Combined, ColumnsScatter::COMBINING_ENABLED)
                    << shape.name << ", width " << width << ", " << pieces << " pieces";

            const ColumnPtr source = shape.make(rows);
            const ColumnPtr empty_piece = source->cloneEmpty();
            for (size_t kind = 0; kind < 5; ++kind)
            {
                const auto ids = combiningIds<Id>(kind, rows, pieces);
                const PieceRows piece_rows(ids, pieces);
                const std::array<const IColumn *, 1> sources{source.get()};
                const std::array<std::span<const Id>, 1> id_spans{std::span<const Id>(ids.data(), ids.size())};
                const auto counts = ColumnsScatter::countRowsPerPiece<Id>(id_spans, pieces);
                const auto result = ColumnsScatter::scatter<Id>(sources, id_spans, counts, true);
                ASSERT_EQ(result.size(), pieces) << shape.name;
                for (size_t piece = 0; piece < pieces; ++piece)
                {
                    const auto piece_row_ids = piece_rows.of(piece);
                    ASSERT_EQ(result[piece]->size(), piece_row_ids.size()) << shape.name << ", kind " << kind << ", piece " << piece;
                    if (!comparedInFull(piece, pieces, piece_row_ids.size()))
                        continue;
                    const ColumnPtr oracle = piece_row_ids.empty() ? empty_piece : ColumnPtr(insertFromPiece(*source, piece_row_ids));
                    ASSERT_TRUE(sameColumn(*oracle, *result[piece], true))
                        << shape.name << ", " << pieces << " pieces, kind " << kind << ", piece " << piece;
                    ASSERT_TRUE(exactPiece(*result[piece], *oracle))
                        << shape.name << ", " << pieces << " pieces, kind " << kind << ", piece " << piece;
                }
            }
        }
    }
}

/// The first line of a piece starts at its cursor, which can be any multiple of the width inside a 64-byte line. In the
/// combined mode, each row must land where the direct mode puts it, and the bytes around each piece must stay as they
/// were: neither the regular stores of the first line nor the non-temporal stores of the full lines may spill.
TEST(ColumnScatter, MisalignedCursorSeeding)
{
    constexpr size_t fanout = 128;
    constexpr size_t rows = 20000;
    constexpr size_t guard = 64;
    pcg64 rng(selector_seed);

    for (const size_t width : {1, 2, 4, 8, 16})
    {
        PaddedPODArray<char> data;
        data.resize(rows * width);
        for (char & byte : data)
            byte = static_cast<char>(rng());
        const auto pids = randomIds<UInt16>(rng, rows, fanout);
        const PieceRows piece_rows(pids, fanout);

        size_t most_rows = 0;
        for (size_t piece = 0; piece < fanout; ++piece)
            most_rows = std::max(most_rows, piece_rows.of(piece).size());
        const size_t stride = (guard + most_rows * width + 64 + guard + 63) / 64 * 64;

        PaddedPODArray<char> memory;
        memory.resize(fanout * stride + 64);
        std::memset(memory.data(), 0xAA, memory.size());
        char * base = reinterpret_cast<char *>((reinterpret_cast<uintptr_t>(memory.data()) + 63) & ~uintptr_t{63});

        std::vector<char *> starts(fanout);
        ColumnsScatter::ScatterScratch scratch;
        scratch.init(fanout, ColumnsScatter::StoreMode::Combined);
        for (size_t piece = 0; piece < fanout; ++piece)
        {
            /// Every multiple of the width inside a line, in turn.
            starts[piece] = base + piece * stride + guard + (piece % (ColumnsScatter::LINE_BYTES / width)) * width;
            scratch.seed(piece, starts[piece]);
        }

        for (const auto & [offset, length] : std::to_array<std::pair<size_t, size_t>>({{0, 6000}, {6000, 1}, {6001, rows - 6001}}))
            ColumnsScatter::scatterPidChunk(
                width, pids.data() + offset, data.data() + offset * width, length, ColumnsScatter::StoreMode::Combined, scratch);
        scratch.drain();

        PaddedPODArray<char> expected;
        expected.resize(memory.size());
        std::memset(expected.data(), 0xAA, expected.size());
        char * expected_base = expected.data() + (base - memory.data());
        for (size_t piece = 0; piece < fanout; ++piece)
        {
            char * out = expected_base + (starts[piece] - base);
            for (const UInt32 row : piece_rows.of(piece))
            {
                std::memcpy(out, data.data() + row * width, width);
                out += width;
            }
        }
        ASSERT_TRUE(sameBytes(bytesOf(expected), bytesOf(memory))) << "width " << width;
    }
}

#if !defined(DEBUG_OR_SANITIZER_BUILD)
/// Each misuse of the chunk API throws `LOGICAL_ERROR`, which aborts Debug and sanitizer builds. So the test runs in
/// Release builds only.
TEST(ColumnScatter, ChunkMisuse)
{
    using ColumnsScatter::StoreMode;
    alignas(64) char memory[1024] = {};
    const std::vector<UInt16> pids(16, 1);
    std::vector<UInt64> data(16, 7);
    const char * rows = reinterpret_cast<const char *>(data.data());

    const auto seed_all = [&](ColumnsScatter::ScatterScratch & scratch)
    {
        for (size_t piece = 0; piece < 4; ++piece)
            scratch.seed(piece, memory + piece * 256);
    };

    ColumnsScatter::ScatterScratch direct;
    direct.init(4, StoreMode::Direct);
    seed_all(direct);
    expectErrorCode(ErrorCodes::LOGICAL_ERROR, "combined mode without staging lines", [&] { direct.setStoreMode(StoreMode::Combined); });
    expectErrorCode(
        ErrorCodes::LOGICAL_ERROR,
        "combined call on a direct scratch",
        [&] { ColumnsScatter::scatterPidChunk(8, pids.data(), rows, 16, StoreMode::Combined, direct); });

    ColumnsScatter::ScatterScratch combined;
    combined.init(4, StoreMode::Combined);
    seed_all(combined);
    expectErrorCode(
        ErrorCodes::LOGICAL_ERROR,
        "direct call on a combined scratch",
        [&] { ColumnsScatter::scatterPidChunk(8, pids.data(), rows, 16, StoreMode::Direct, combined); });
    for (const size_t width : {3, 32})
        expectErrorCode(
            ErrorCodes::LOGICAL_ERROR,
            "combined call with an unsupported width",
            [&] { ColumnsScatter::scatterPidChunk(width, pids.data(), rows, 2, StoreMode::Combined, combined); });

    combined.seed(2, memory + 256 * 2 + 4);
    expectErrorCode(
        ErrorCodes::LOGICAL_ERROR,
        "combined call with a cursor that is not a multiple of the width",
        [&] { ColumnsScatter::scatterPidChunk(8, pids.data(), rows, 16, StoreMode::Combined, combined); });

    /// With the cursor back on a multiple of the width, the same call works.
    combined.seed(2, memory + 256 * 2);
    ColumnsScatter::scatterPidChunk(8, pids.data(), rows, 16, StoreMode::Combined, combined);
    combined.drain();
    EXPECT_EQ(std::memcmp(memory + 256, data.data(), 16 * sizeof(UInt64)), 0);
}
#endif

/// A scratch serves the next column after `drain`, also when that column has another width and gives rows to fewer
/// pieces. The pieces that get no rows keep no cursor of the column before, so its misaligned cursors do not fail the
/// alignment check of the next one.
TEST(ColumnScatter, ChunkScratchReuse)
{
    using ColumnsScatter::StoreMode;
    constexpr size_t fanout = 8;
    alignas(64) char memory[fanout * 256] = {};
    pcg64 rng(selector_seed);

    ColumnsScatter::ScatterScratch scratch;
    scratch.init(fanout, StoreMode::Combined);

    /// A column of 2-byte rows, with cursors that are multiples of 2 and not of 8.
    const std::vector<UInt16> narrow_pids(64, 1);
    const std::vector<UInt16> narrow_rows(64, 0x0102);
    for (size_t piece = 0; piece < fanout; ++piece)
        scratch.seed(piece, memory + piece * 256 + 2);
    ColumnsScatter::scatterPidChunk(
        2, narrow_pids.data(), reinterpret_cast<const char *>(narrow_rows.data()), 64, StoreMode::Combined, scratch);
    scratch.drain();
    for (const char * cursor : scratch.cursors)
        EXPECT_EQ(cursor, nullptr);

    /// A column of 8-byte rows that all go to piece 3, the only piece that is seeded.
    std::vector<UInt64> wide_rows(100);
    for (auto & row : wide_rows)
        row = rng();
    const std::vector<UInt16> wide_pids(100, 3);
    scratch.seed(3, memory + 3 * 256 + 8);
    ColumnsScatter::scatterPidChunk(
        8, wide_pids.data(), reinterpret_cast<const char *>(wide_rows.data()), 100, StoreMode::Combined, scratch);
    scratch.drain();
    EXPECT_EQ(std::memcmp(memory + 3 * 256 + 8, wide_rows.data(), 100 * sizeof(UInt64)), 0);
}

/// `init` makes a scratch ready for another number of pieces and another mode, and `setStoreMode` switches the mode of a
/// scratch that has staging lines. After each, the scratch scatters a column in its mode.
TEST(ColumnScatter, ChunkScratchReinit)
{
    using ColumnsScatter::StoreMode;
    ColumnsScatter::ScatterScratch scratch;
    EXPECT_EQ(scratch.mode(), StoreMode::Direct);

    /// The pieces take 10 rows of 8 bytes each, in turn. Each piece has 128 bytes and starts 16 bytes into them.
    const auto scatter_and_check = [&scratch](size_t fanout, StoreMode mode)
    {
        EXPECT_EQ(scratch.mode(), mode);
        EXPECT_EQ(scratch.cursors.size(), fanout);
        std::vector<UInt64> rows(fanout * 10);
        std::vector<UInt16> pids(rows.size());
        for (size_t i = 0; i < rows.size(); ++i)
        {
            rows[i] = i;
            pids[i] = static_cast<UInt16>(i % fanout);
        }
        std::vector<UInt64> memory(fanout * 16 + 2);
        char * const base = reinterpret_cast<char *>(memory.data()) + 16;
        for (size_t piece = 0; piece < fanout; ++piece)
            scratch.seed(piece, base + piece * 128);
        ColumnsScatter::scatterPidChunk(8, pids.data(), reinterpret_cast<const char *>(rows.data()), rows.size(), mode, scratch);
        scratch.drain();
        for (size_t piece = 0; piece < fanout; ++piece)
            for (size_t k = 0; k < 10; ++k)
            {
                UInt64 value = 0;
                memcpy(&value, base + piece * 128 + k * 8, sizeof(value));
                ASSERT_EQ(value, piece + k * fanout) << "mode " << static_cast<int>(mode) << ", " << fanout << " pieces, piece " << piece;
            }
    };

    for (const auto [fanout, mode] : std::vector<std::pair<size_t, StoreMode>>{
             {256, StoreMode::Combined},
             {300, StoreMode::Direct},
             {1024, StoreMode::Combined},
             {1024, StoreMode::Direct},
             {256, StoreMode::Combined},
             {256, StoreMode::Combined},
             {17, StoreMode::Direct}})
    {
        scratch.init(fanout, mode);
        scatter_and_check(fanout, mode);
    }

    scratch.init(512, StoreMode::Combined);
    scratch.setStoreMode(StoreMode::Direct);
    scatter_and_check(512, StoreMode::Direct);
    scratch.setStoreMode(StoreMode::Combined);
    scatter_and_check(512, StoreMode::Combined);

    scratch.init(0, StoreMode::Direct);
    EXPECT_EQ(scratch.cursors.size(), 0);
    EXPECT_EQ(scratch.mode(), StoreMode::Direct);
    scratch.init(0, StoreMode::Combined);
    EXPECT_EQ(scratch.cursors.size(), 0);
    EXPECT_EQ(scratch.mode(), StoreMode::Combined);
    scratch.drain();
}

/// Several sources of fixed-width columns, at shapes where `chooseStoreMode` turns write-combining on, give with the hint
/// the pieces that `scatter` gives for each source alone. The staging lines carry rows from one source to the next.
TYPED_TEST(ColumnScatterByIdType, BatchedWriteCombining)
{
    expectSameAsPerSource<TypeParam>(
        {
            {.name = "UInt64", .makes = {randomFixed<ColumnUInt64>(), randomFixed<ColumnUInt64>()}, .rows = {3000, 2500}},
            {.name = "UInt16", .makes = {randomFixed<ColumnUInt16>(), randomFixed<ColumnUInt16>()}, .rows = {9000, 8000}},
            {.name = "UInt8", .makes = {randomFixed<ColumnUInt8>(), randomFixed<ColumnUInt8>()}, .rows = {20000, 15000}},
            {.name = "Nullable(UInt64)",
             .makes = {nullableOf(randomFixed<ColumnUInt64>()), nullableOf(randomFixed<ColumnUInt64>())},
             .rows = {20000, 15000}},
        },
        256,
        /*allow_combining=*/true);
}
