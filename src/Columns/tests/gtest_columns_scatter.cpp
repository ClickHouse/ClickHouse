#include <AggregateFunctions/AggregateFunctionFactory.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnBLOB.h>
#include <Columns/ColumnCompressed.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDynamic.h>
#include <Columns/ColumnFunction.h>
#include <Columns/ColumnLazy.h>
#include <Columns/ColumnLowCardinality.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnNothing.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnReplicated.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnSparse.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnUnique.h>
#include <Columns/ColumnVariant.h>
#include <Columns/ColumnsCommon.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumnDummy.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/IFunction.h>
#include <Functions/IFunctionAdaptors.h>
#include <Common/Exception.h>
#include <Common/assert_cast.h>
#include <Common/tests/gtest_global_register.h>
#include <Common/typeid_cast.h>

#include <fmt/format.h>
#include <gtest/gtest.h>
#include <pcg_random.hpp>

#include <array>
#include <cstring>
#include <functional>
#include <limits>
#include <map>
#include <numeric>
#include <span>
#include <string_view>
#include <unordered_set>
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
    /// The columns that are slow to clone or to filter run only these cases: at most 1000 rows and 1024 pieces,
    /// plus one case with 65536 pieces.
    bool in_capped_set = false;
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

    /// The offsets of a `Variant` are not compared: for NULL rows their value is unspecified.
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
    /// What the default `scatter` does: `cloneEmpty`, then one `insertFrom` per row, in row order.
    InsertFrom,
    /// A top-level `LowCardinality`: the source dictionary, shared, with the indexes of the piece's rows.
    SharedDictionary,
    /// A type with its own `scatter`: `filter` with the mask of the piece.
    Filter,
    /// `AggregateFunction`: views that point to the source states.
    SourceStates,
};

struct SourceSpec
{
    String name;
    std::function<ColumnPtr(size_t rows)> make;
    PieceOracle oracle = PieceOracle::InsertFrom;
    /// Cheap to clone: runs every selector case. The others run the capped set.
    bool every_case = false;
    /// The column holds a `LowCardinality` below the top level, and each piece must get its own dictionary.
    bool own_dictionaries = false;
    /// Must hold for a source of at least 3 rows, so that the case takes the path it is named after.
    std::function<bool(const IColumn & source)> on_path = {};
    /// A check of one piece beyond `oracle`.
    std::function<testing::AssertionResult(const IColumn & source, const IColumn & piece, std::span<const UInt32> rows)> extra = {};
};

/// The pieces that are compared in full: every piece with rows, the first 1024 and the last one. The other empty
/// pieces are only checked for their representation. This keeps the cases with 65536 pieces fast.
bool comparedInFull(size_t piece, size_t num_pieces, size_t piece_size)
{
    return piece_size != 0 || piece < 1024 || piece + 1 == num_pieces;
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
            auto column = source.cloneEmpty();
            for (const auto row : rows)
                column->insertFrom(source, row);
            result[piece] = std::move(column);
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

/// 301 dictionary values, so the indexes are `UInt16` at any row count.
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

    specs.push_back({.name = "Const(UInt64)", .make = makeConst, .every_case = true, .extra = sharesConstData});
    specs.push_back({.name = "Nothing", .make = [](size_t rows) -> ColumnPtr { return ColumnNothing::create(rows); }, .every_case = true});
    specs.push_back(
        {.name = "Set", .make = [](size_t rows) -> ColumnPtr { return ColumnSet::create(rows, FutureSetPtr{}); }, .every_case = true});

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
}

/// The columns that cannot be scattered throw from both `scatter` and `scatterWithCounts`, with these error codes.
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

}

template <typename Id>
class ColumnScatterByIdType : public ::testing::Test
{
};

using IdTypes = ::testing::Types<UInt64, UInt16>;
TYPED_TEST_SUITE(ColumnScatterByIdType, IdTypes);

/// Every piece must equal its oracle in values and in representation. A piece that loses the representation of its
/// source, such as `Sparse`, fails. An override that passes its counts to a nested column with another selector fails the sum
/// check, or the recount in Debug and sanitizer builds.
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
            if (!spec.every_case && !selector_case.in_capped_set)
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

    /// 16-bit ids address at most 65536 pieces.
    if constexpr (std::is_same_v<TypeParam, UInt16>)
    {
        expectErrorCode(
            ErrorCodes::LOGICAL_ERROR, "countColumnsSizeInSelector", [&] { (void)countColumnsSizeInSelector(65537, selector); });
        expectErrorCode(ErrorCodes::LOGICAL_ERROR, "UInt64", [&] { (void)makeNumbers(rows)->scatter(65537, selector); });
        expectErrorCode(ErrorCodes::LOGICAL_ERROR, "Const", [&] { (void)makeConst(rows)->scatter(65537, selector); });
    }
}
#endif

/// 65536 pieces is the most that 16-bit ids address. Both entries must accept it, and the rows with id 65535 must
/// reach the last piece in row order. An off-by-one in the bound check would reject the count or lose the last piece.
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

    const auto numbers = makeNumbers(rows);
    const auto strings = makeStrings(rows);
    for (const auto & column : {numbers, strings})
    {
        for (const bool with_counts : {false, true})
        {
            const auto pieces = with_counts ? column->scatterWithCounts(selector, counts) : column->scatter(num_pieces, selector);
            const String where = fmt::format("{}, {}", column->getName(), with_counts ? "scatterWithCounts" : "scatter");
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
