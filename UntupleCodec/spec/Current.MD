# Current Codec Handling

## Summary

ClickHouse stores a column's declared compression codec in table metadata, not in `IColumn`, `IDataType`, or the data part's column type list.

For a `Tuple` column, one codec expression belongs to the top-level column. When a MergeTree part is written, the tuple serialization produces a separate physical stream for each element, but the writer resolves every stream from the same top-level codec expression.

```text
CREATE TABLE / ALTER TABLE
        |
        v
ASTColumnDeclaration::codec
        |
        v
ColumnDescription::codec
        |
        v
StorageInMemoryMetadata::columns
        |
        v
MergeTree writer enumerates serialization streams
        |
        v
CompressionCodec instance per physical stream
```

## `CREATE TABLE`

Given:

```sql
CREATE TABLE example
(
    value Tuple(number UInt64, text String) CODEC(Delta, ZSTD)
)
ENGINE = MergeTree
ORDER BY tuple();
```

the parser creates an `ASTColumnDeclaration` for `value`. Its type and codec are separate children:

```text
ASTColumnDeclaration "value"
|- type:  Tuple(number UInt64, text String)
`- codec: CODEC(Delta, ZSTD)
```

`InterpreterCreateQuery` validates and preprocesses the codec through `CompressionCodecFactory::validateCodecAndGetPreprocessedAST`. The result is stored in:

```cpp
struct ColumnDescription
{
    String name;
    DataTypePtr type;
    ASTPtr codec;
};
```

The `ColumnDescription` is part of `ColumnsDescription`, which is held by `StorageInMemoryMetadata`.

The codec remains an AST instead of a persistent `CompressionCodecPtr` because the final codec can depend on the part default, a `RECOMPRESS` TTL, and the data type of an individual serialization stream.

The declaration is persisted as part of the normalized table definition. It is also included in the textual column metadata used by replicated tables.

## Why `IColumn` Does Not Contain the Codec

`IColumn` represents in-memory values. Compression is a schema and storage-writing policy.

The same `IColumn` can be:

- Used during expression evaluation without storage compression.
- Written into different tables with different codecs.
- Read from a part using one codec and later written into a new part using another.
- Written under a part default selected by a TTL recompression operation.
- Sent through a format or protocol unrelated to MergeTree compression.

The column's name and type are also normally attached outside `IColumn`, for example in `ColumnWithTypeAndName`. Codec ownership belongs one layer farther out, in storage metadata.

## Tuple Serialization

`DataTypeTuple` constructs a `SerializationTuple`. Every tuple element is wrapped in a `SerializationNamed` with `SubstreamType::TupleElement`.

`SerializationTuple::enumerateStreams` visits the elements independently and builds a typed `ISerialization::SubstreamPath`. For the example above, the writer eventually sees paths corresponding to:

```text
value.number
value.text
```

An element with a compound type can produce more than one stream. Examples include array offsets, nullable maps, low-cardinality dictionaries, and nested tuple elements.

At this stage, the serialization path contains both the tuple element identity and the leaf data type. This is the first layer with enough information to select a codec for an individual tuple element.

## MergeTree Writing

`MergeTreeDataPartWriterOnDisk::initStreamsIfNeeded` iterates over `columns_list`. For an ordinary tuple declaration, the entry is the top-level storage column:

```text
name: value
type: Tuple(number UInt64, text String)
```

It is not an entry for `value.number` or `value.text`.

The writer obtains one effective codec expression:

```cpp
auto compression = getCodecDescOrDefault(column.name, default_codec);
addStreams(column, compression);
```

`getCodecDescOrDefault` reads `ColumnDescription::codec`, or returns the resolved part default when no codec is declared.

Both `MergeTreeDataPartWriterWide::addStreams` and `MergeTreeDataPartWriterCompact::addStreams` enumerate the physical streams. For each stream they instantiate a codec using the same `effective_codec_desc`, but pass the stream's leaf subtype to `CompressionCodecFactory::get`:

```text
value.number -> CODEC(Delta, ZSTD), instantiated for UInt64
value.text   -> CODEC(Delta, ZSTD), instantiated for String
```

Therefore codec objects are already created per physical stream, but all streams of a top-level column are currently derived from one codec AST.

Structural streams are treated specially. Codecs that depend on the value type are not applied to streams such as array offsets and null maps; the writer requests generic codecs only for those streams.

Wide parts create separate compressed files for streams. Compact parts can place multiple logical streams in a shared data file, but still maintain codec-aware compressed streams internally. Both writers have the same top-level-codec limitation.

## `ALTER TABLE ... CODEC`

For:

```sql
ALTER TABLE example
    MODIFY COLUMN value CODEC(ZSTD);
```

the `ALTER` parser creates an `ASTAlterCommand::MODIFY_COLUMN` containing an `ASTColumnDeclaration`:

```text
name:  value
type:  absent
codec: CODEC(ZSTD)
```

`AlterCommand::parse` transfers the name and codec into an `AlterCommand`. `AlterCommand::apply` then modifies the existing `ColumnDescription::codec`, validating against the existing column type when the query does not repeat the type.

A codec-only `ALTER` is metadata-only. It does not schedule a mutation and does not immediately rewrite existing parts. The new declaration affects:

- Newly inserted parts.
- Parts written by later merges.
- Parts rewritten by later mutations.

Existing parts remain readable because every compressed block identifies its actual codec. Readers obtain the decompressor from the block header rather than from the table's current codec declaration.

After metadata has been updated, there is no distinction in the writer between a codec originating from `CREATE TABLE` and one originating from `ALTER TABLE`.

## Current Limitation

The limitation is the metadata-to-writer interface:

```text
ColumnDescription::codec contains one AST
        |
        v
initStreamsIfNeeded resolves it once for the top-level column
        |
        v
addStreams applies it to every descendant stream
```

`SerializationTuple` already exposes tuple-element paths and leaf types. The missing piece is a schema-level codec policy that can resolve a different codec AST for each logical subcolumn path.
