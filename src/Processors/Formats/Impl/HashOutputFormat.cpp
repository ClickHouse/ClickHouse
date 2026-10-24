#include <Processors/Formats/Impl/HashOutputFormat.h>

#include <Columns/IColumn.h>
#include <Columns/canonicalizeNegativeZero.h>
#include <Common/Arena.h>
#include <Core/Block.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <Formats/FormatFactory.h>
#include <Formats/FormatSettings.h>
#include <IO/WriteBuffer.h>
#include <Processors/Port.h>


namespace DB
{

HashOutputFormat::HashOutputFormat(WriteBuffer & out_, SharedHeader header_)
    : IOutputFormat(header_, out_)
{
}

String HashOutputFormat::getName() const
{
    return "HashOutputFormat";
}

void HashOutputFormat::consume(Chunk chunk)
{
    /// The hash of a value is the hash of a hash table key, which agrees with `equals`, so it is the same
    /// for `-0.` and `0.`. The fingerprint of a result has to tell the two apart, because they are
    /// different values, so a value that contains a negative zero also contributes its serialized
    /// representation. Every other value hashes exactly as before this distinction was needed.
    const Columns & columns = chunk.getColumns();
    const size_t num_columns = columns.size();

    /// `nullptr` for a column without a negative zero, which is by far the most common case.
    Columns canonical_columns(num_columns);
    Columns full_columns(num_columns);
    /// The column whose serialized values tell the two zeros apart: `convertToFullIfWrapped` keeps
    /// `LowCardinality`, whose dictionary can hold a negative zero as well, and `canonicalizeNegativeZero`
    /// does not look into it, so the check is done on the column without `LowCardinality`.
    Columns value_columns(num_columns);
    for (size_t j = 0; j < num_columns; ++j)
    {
        full_columns[j] = columns[j]->convertToFullIfWrapped();
        value_columns[j] = recursiveRemoveLowCardinality(full_columns[j]);
        canonical_columns[j] = canonicalizeNegativeZero(*value_columns[j]);
    }

    Arena arena;
    for (size_t i = 0, rows = chunk.getNumRows(); i < rows; ++i)
    {
        for (size_t j = 0; j < num_columns; ++j)
        {
            full_columns[j]->updateHashWithValue(i, hash);

            if (!canonical_columns[j])
                continue;

            const char * begin = nullptr;
            const std::string_view value = value_columns[j]->serializeValueIntoArena(i, arena, begin, nullptr);
            begin = nullptr;
            const std::string_view canonical_value = canonical_columns[j]->serializeValueIntoArena(i, arena, begin, nullptr);
            if (value != canonical_value)
                hash.update(value.data(), value.size());

            /// Both values are only needed for the comparison; release them in the reverse order of
            /// allocation, so that the arena does not grow with the number of rows.
            arena.rollback(canonical_value.size());
            arena.rollback(value.size());
        }
    }
}

void HashOutputFormat::finalizeImpl()
{
    std::string hash_string = getSipHash128AsHexString(hash);
    out.write(hash_string.data(), hash_string.size());
    out.write("\n", 1);
    out.next();
}

void registerOutputFormatHash(FormatFactory & factory);
void registerOutputFormatHash(FormatFactory & factory)
{
    factory.registerOutputFormat("Hash",
        [](WriteBuffer & buf, const Block & header, const FormatSettings &, FormatFilterInfoPtr /*format_filter_info*/)
        {
            return std::make_shared<HashOutputFormat>(buf, std::make_shared<const Block>(header));
        });

    factory.setDocumentation("Hash", Documentation{
        .description = R"DOCS_MD(
| Input | Output | Alias |
|-------|--------|-------|
| ✗     | ✔      |       |

## Description {#description}

The `Hash` output format calculates a single hash value for all columns and rows of the result.
This is useful for calculating a "fingerprint" of the result, for example in situations where data transfer is the bottleneck.

## Example usage {#example-usage}

### Reading data {#reading-data}

Consider a table `football` with the following data:

```text
    ┌───────date─┬─season─┬─home_team─────────────┬─away_team───────────┬─home_team_goals─┬─away_team_goals─┐
 1. │ 2022-04-30 │   2021 │ Sutton United         │ Bradford City       │               1 │               4 │
 2. │ 2022-04-30 │   2021 │ Swindon Town          │ Barrow              │               2 │               1 │
 3. │ 2022-04-30 │   2021 │ Tranmere Rovers       │ Oldham Athletic     │               2 │               0 │
 4. │ 2022-05-02 │   2021 │ Port Vale             │ Newport County      │               1 │               2 │
 5. │ 2022-05-02 │   2021 │ Salford City          │ Mansfield Town      │               2 │               2 │
 6. │ 2022-05-07 │   2021 │ Barrow                │ Northampton Town    │               1 │               3 │
 7. │ 2022-05-07 │   2021 │ Bradford City         │ Carlisle United     │               2 │               0 │
 8. │ 2022-05-07 │   2021 │ Bristol Rovers        │ Scunthorpe United   │               7 │               0 │
 9. │ 2022-05-07 │   2021 │ Exeter City           │ Port Vale           │               0 │               1 │
10. │ 2022-05-07 │   2021 │ Harrogate Town A.F.C. │ Sutton United       │               0 │               2 │
11. │ 2022-05-07 │   2021 │ Hartlepool United     │ Colchester United   │               0 │               2 │
12. │ 2022-05-07 │   2021 │ Leyton Orient         │ Tranmere Rovers     │               0 │               1 │
13. │ 2022-05-07 │   2021 │ Mansfield Town        │ Forest Green Rovers │               2 │               2 │
14. │ 2022-05-07 │   2021 │ Newport County        │ Rochdale            │               0 │               2 │
15. │ 2022-05-07 │   2021 │ Oldham Athletic       │ Crawley Town        │               3 │               3 │
16. │ 2022-05-07 │   2021 │ Stevenage Borough     │ Salford City        │               4 │               2 │
17. │ 2022-05-07 │   2021 │ Walsall               │ Swindon Town        │               0 │               3 │
    └────────────┴────────┴───────────────────────┴─────────────────────┴─────────────────┴─────────────────┘
```

Read data using the `Hash` format:

```sql
SELECT *
FROM football
FORMAT Hash
```

The query will process the data, but will not output anything.

```response
df2ec2f0669b000edff6adee264e7d68

1 rows in set. Elapsed: 0.154 sec.
```

## Format settings {#format-settings}
)DOCS_MD"});
}

}
