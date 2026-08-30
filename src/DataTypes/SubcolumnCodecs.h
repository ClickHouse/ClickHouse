#pragma once

#include <Core/Types.h>
#include <Parsers/IAST_fwd.h>

#include <map>


namespace DB
{

class IDataType;
using DataTypePtr = std::shared_ptr<const IDataType>;
struct CodecValidationSettings;

/// Codecs declared for elements of a Tuple inside a column type, for example:
///     CREATE TABLE t
///     (
///         samples Array(Tuple(
///             timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)),
///             value Float64 CODEC(Gorilla, ZSTD(1))))
///     ) ENGINE = MergeTree ...
/// The key is the subcolumn name of the element relative to the column (as in `SELECT samples.timestamp`),
/// e.g. "timestamp" or "a.b" for an element of a nested tuple.
/// Such codecs are not part of the data type: they are extracted from the type AST when a column
/// declaration is processed and stored in ColumnDescription::subcolumn_codecs. The MergeTree part
/// writers use them for the streams of the corresponding subcolumns instead of the column-level codec.
using SubcolumnCodecs = std::map<String, ASTPtr>;

/// Whether the type AST contains codecs of tuple elements.
bool typeASTHasSubcolumnCodecs(const IAST & type_ast);

/// Collects codecs of tuple elements from the type AST into `out_codecs`, keyed by subcolumn name.
/// Returns the type AST without the codecs: the original AST if it has none, or a cleaned clone
/// (the original AST is never modified because it can be a part of a query which is formatted later).
ASTPtr extractSubcolumnCodecsFromTypeAST(const ASTPtr & type_ast, SubcolumnCodecs & out_codecs);

/// Same as extractSubcolumnCodecsFromTypeAST() but discards the codecs.
ASTPtr typeASTWithoutSubcolumnCodecs(const ASTPtr & type_ast);

/// Puts the codecs back into a type AST without codecs; the reverse of extractSubcolumnCodecsFromTypeAST().
/// Used to format a column declaration for SHOW CREATE TABLE and for table metadata.
void injectSubcolumnCodecsIntoTypeAST(IAST & type_ast, const SubcolumnCodecs & codecs);

/// Returns the name of the type annotated with the codecs,
/// e.g. "Array(Tuple(timestamp DateTime64(3, 'UTC') CODEC(DoubleDelta, ZSTD(1)), value Float64))".
String getTypeNameWithSubcolumnCodecs(const DataTypePtr & type, const SubcolumnCodecs & codecs);

/// Checks that every declared subcolumn exists in the column type and that its codec is applicable
/// to the type of that subcolumn; replaces each codec with its validated preprocessed form.
void validateSubcolumnCodecs(
    const String & column_name,
    const DataTypePtr & column_type,
    SubcolumnCodecs & codecs,
    const CodecValidationSettings & validation_settings);

/// Deep copy: the codec ASTs are cloned.
SubcolumnCodecs cloneSubcolumnCodecs(const SubcolumnCodecs & codecs);

}
