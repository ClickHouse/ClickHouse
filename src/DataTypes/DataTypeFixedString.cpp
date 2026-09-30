#include <Columns/ColumnFixedString.h>

#include <Common/Exception.h>
#include <Common/SipHash.h>

#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/Serializations/SerializationFixedString.h>
#include <DataTypes/Serializations/SerializationFixedStringWithTextRepresentation.h>

#include <IO/WriteHelpers.h>

#include <Poco/String.h>

#include <Parsers/IAST.h>
#include <Parsers/ASTLiteral.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ARGUMENT_OUT_OF_BOUND;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int UNEXPECTED_AST_STRUCTURE;
}

String fixedStringTextRepresentationToString(FixedStringTextRepresentation representation)
{
    switch (representation)
    {
        case FixedStringTextRepresentation::Raw: return "Raw";
        case FixedStringTextRepresentation::Hex: return "Hex";
        case FixedStringTextRepresentation::Base64: return "Base64";
        case FixedStringTextRepresentation::Base64URL: return "Base64URL";
        case FixedStringTextRepresentation::Base58: return "Base58";
    }
    UNREACHABLE();
}

FixedStringTextRepresentation parseFixedStringTextRepresentation(const String & representation)
{
    static constexpr FixedStringTextRepresentation all_representations[] = {
        FixedStringTextRepresentation::Raw,
        FixedStringTextRepresentation::Hex,
        FixedStringTextRepresentation::Base64,
        FixedStringTextRepresentation::Base64URL,
        FixedStringTextRepresentation::Base58,
    };

    for (auto candidate : all_representations)
        if (Poco::icompare(representation, fixedStringTextRepresentationToString(candidate)) == 0)
            return candidate;

    throw Exception(ErrorCodes::BAD_ARGUMENTS,
        "Unknown FixedString text representation '{}'. Supported values are Raw, Hex, Base64, Base64URL, Base58", representation);
}

DataTypeFixedString::DataTypeFixedString(size_t n_, FixedStringTextRepresentation text_representation_) : n(n_), text_representation(text_representation_)
{
    if (n == 0)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "FixedString size must be positive");
    if (n > MAX_FIXEDSTRING_SIZE)
        throw Exception(ErrorCodes::ARGUMENT_OUT_OF_BOUND, "FixedString size is too large");
}

std::string DataTypeFixedString::doGetName() const
{
    if (text_representation == FixedStringTextRepresentation::Raw)
        return "FixedString(" + toString(n) + ")";

    return "FixedString(" + toString(n) + ", '" + fixedStringTextRepresentationToString(text_representation) + "')";
}

MutableColumnPtr DataTypeFixedString::createColumn() const
{
    return ColumnFixedString::create(n);
}

Field DataTypeFixedString::getDefault() const
{
    return String();
}

bool DataTypeFixedString::equals(const IDataType & rhs) const
{
    return typeid(rhs) == typeid(*this)
        && n == static_cast<const DataTypeFixedString &>(rhs).n
        && text_representation == static_cast<const DataTypeFixedString &>(rhs).text_representation;
}

void DataTypeFixedString::updateHashImpl(SipHash & hash) const
{
    hash.update(n);
    hash.update(static_cast<UInt8>(text_representation));
}

SerializationPtr DataTypeFixedString::doGetSerialization(const SerializationInfoSettings &) const
{
    if (text_representation == FixedStringTextRepresentation::Raw)
        return SerializationFixedString::create(n);

    return SerializationFixedStringWithTextRepresentation::create(text_representation, n);
}


static DataTypePtr create(const ASTPtr & arguments)
{
    if (!arguments || (arguments->children.size() != 1 && arguments->children.size() != 2))
        throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                        "FixedString data type family must have one or two arguments: size in bytes and optional text representation");

    const auto * argument = arguments->children[0]->as<ASTLiteral>();
    if (!argument || argument->value.getType() != Field::Types::UInt64 || argument->value.safeGet<UInt64>() == 0)
        throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                        "FixedString data type family must have a number (positive integer) as its first argument");

    FixedStringTextRepresentation text_representation = FixedStringTextRepresentation::Raw;
    if (arguments->children.size() == 2)
    {
        const auto * representation_argument = arguments->children[1]->as<ASTLiteral>();
        if (!representation_argument || representation_argument->value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::UNEXPECTED_AST_STRUCTURE,
                            "The second argument of FixedString data type family must be a string literal with text representation");

        text_representation = parseFixedStringTextRepresentation(representation_argument->value.safeGet<String>());
    }

    return std::make_shared<DataTypeFixedString>(argument->value.safeGet<UInt64>(), text_representation);
}


void registerDataTypeFixedString(DataTypeFactory & factory)
{
    factory.registerDataType("FixedString", create, DataTypeFactory::Case::Sensitive,
        Documentation{
            .description = R"DOCS_MD(
A fixed-length string of `N` bytes (neither characters nor code points).

To declare a column of `FixedString` type, use the following syntax:

```sql
<column_name> FixedString(N)
```

Where `N` is a natural number.

The `FixedString` type is efficient when data has the length of precisely `N` bytes. In all other cases, it is likely to reduce efficiency.

Examples of the values that can be efficiently stored in `FixedString`-typed columns:

- The binary representation of IP addresses (`FixedString(16)` for IPv6).
- Language codes (ru_RU, en_US, ...).
- Currency codes (USD, RUB, ...).
- Binary representation of hashes (`FixedString(16)` for MD5, `FixedString(32)` for SHA256).

To store UUID values, use the [UUID](/reference/data-types/uuid) data type.

When inserting the data, ClickHouse:

- Complements a string with null bytes if the string contains fewer than `N` bytes.
- Throws the `Too large value for FixedString(N)` exception if the string contains more than `N` bytes.

Let's consider the following table with the single `FixedString(2)` column:

```sql


INSERT INTO FixedStringTable VALUES ('a'), ('ab'), ('');
```

```sql
SELECT
    name,
    toTypeName(name),
    length(name),
    empty(name)
FROM FixedStringTable;
```

```text
┌─name─┬─toTypeName(name)─┬─length(name)─┬─empty(name)─┐
│ a    │ FixedString(2)   │            2 │           0 │
│ ab   │ FixedString(2)   │            2 │           0 │
│      │ FixedString(2)   │            2 │           1 │
└──────┴──────────────────┴──────────────┴─────────────┘
```

Note that the length of the `FixedString(N)` value is constant. The [length](/reference/functions/regular-functions/array-functions#length) function returns `N` even if the `FixedString(N)` value is filled only with null bytes, but the [empty](/reference/functions/regular-functions/array-functions#empty) function returns `1` in this case.

Selecting data with `WHERE` clause return various result depending on how the condition is specified:

- If equality operator `=` or `==` or `equals` function used, ClickHouse _doesn't_ take `\0` char into consideration, i.e. queries `SELECT * FROM FixedStringTable WHERE name = 'a';` and `SELECT * FROM FixedStringTable WHERE name = 'a\0';` return the same result.
- If `LIKE` clause is used, ClickHouse _does_ take `\0` char into consideration, so one may need to explicitly specify `\0` char in the filter condition.

```sql
SELECT name
FROM FixedStringTable
WHERE name = 'a'
FORMAT JSONStringsEachRow

{"name":"a\u0000"}


SELECT name
FROM FixedStringTable
WHERE name = 'a\0'
FORMAT JSONStringsEachRow

{"name":"a\u0000"}


SELECT name
FROM FixedStringTable
WHERE name = 'a'
FORMAT JSONStringsEachRow

Query id: c32cec28-bb9e-4650-86ce-d74a1694d79e

{"name":"a\u0000"}


SELECT name
FROM FixedStringTable
WHERE name LIKE 'a'
FORMAT JSONStringsEachRow

0 rows in set.


SELECT name
FROM FixedStringTable
WHERE name LIKE 'a\0'
FORMAT JSONStringsEachRow

{"name":"a\u0000"}
```

## Text representation {#text-representation}

An optional second argument declares how the value is represented as text:

```sql
<column_name> FixedString(N, 'representation')
```

Where `representation` is one of (case-insensitive):

- `'Raw'` — the default, the same as `FixedString(N)`.
- `'Hex'` — hexadecimal digits, `2 * N` characters. On input, an optional `0x` prefix and uppercase digits are accepted. On output, lowercase digits are used.
- `'Base64'` — Base64 with padding, like the [base64Encode](/reference/functions/regular-functions/encoding-functions#base64Encode) function.
- `'Base64URL'` — URL-safe Base64 without padding, like the [base64URLEncode](/reference/functions/regular-functions/encoding-functions#base64URLEncode) function. Padding is optional on input.
- `'Base58'` — Base58 with the Bitcoin alphabet, like the [base58Encode](/reference/functions/regular-functions/encoding-functions#base58Encode) function. Encoding and decoding of 32 and 64 bytes values are specialized.

The value is always stored as exactly `N` raw bytes: the storage, the binary formats (`Native`, `RowBinary`, `Parquet`, ...) and the comparison of values
are the same as for `FixedString(N)`. The representation only changes the conversion from and to text:

- Text input (`INSERT`, text formats, `CAST` from `String`) is decoded, and the decoded value must be exactly `N` bytes, otherwise an exception is thrown. `CAST` to `Nullable` and `accurateCastOrNull` return `NULL` instead.
- Text output (text formats, `toString`, `CAST` to `String`) is encoded in the declared representation.
- A string constant compared with the column (`=`, `!=`, `<`, `IN`, `has`, ...) is decoded once, and the values are compared as bytes. The primary key and skipping indexes are used as for `FixedString(N)`.
- The common type of `FixedString(N, 'representation')` and `String` or `FixedString(N)` is `FixedString(N, 'representation')`. Values with different sizes or different representations cannot be compared or combined without an explicit `CAST`.
- `FixedString(N)` values are converted without decoding, so `toFixedString(base58Decode(s), 32)` can be compared with `FixedString(32, 'Base58')`, and a column can be changed from `FixedString(N)` with `ALTER TABLE ... MODIFY COLUMN` without changing the stored data.
- Functions that accept `FixedString` (`length`, `hex`, `base58Encode`, `LIKE`, `startsWith`, ...) operate on the stored bytes.
- Values are sorted by bytes, which is not the order of the Base58 and Base64 strings.

```sql
CREATE TABLE accounts
(
    id FixedString(32, 'Base58'),
    name String
)
ENGINE = MergeTree ORDER BY id;

INSERT INTO accounts VALUES ('EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v', 'usdc');

SELECT id, hex(id), name FROM accounts WHERE id = 'EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v';
```

```text
┌─id───────────────────────────────────────────┬─hex(id)──────────────────────────────────────────────────────────┬─name─┐
│ EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v │ C6FA7AF3BEDBAD3A3D65F36AABC97431B1BBE4C2D2F6E0E47CA60203452F5D61 │ usdc │
└──────────────────────────────────────────────┴──────────────────────────────────────────────────────────────────┴──────┘
```
)DOCS_MD",
            .syntax = "FixedString(N[, representation])",
            .related = {"String"},
        });

    /// Compatibility alias.
    factory.registerAlias("BINARY", "FixedString", DataTypeFactory::Case::Insensitive);
}

}
