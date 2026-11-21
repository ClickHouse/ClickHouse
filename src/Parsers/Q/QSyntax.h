#pragma once

#include <Core/Field.h>
#include <base/types.h>

#include <memory>
#include <vector>


namespace DB
{

/// The datatypes of q atoms, numbered as in q (`type` returns the negated number for an atom).
enum class QType : int8_t
{
    Mixed = 0,
    Boolean = 1,
    Guid = 2,
    Byte = 4,
    Short = 5,
    Int = 6,
    Long = 7,
    Real = 8,
    Float = 9,
    Char = 10,
    Symbol = 11,
    Timestamp = 12,
    Month = 13,
    Date = 14,
    Datetime = 15,
    Timespan = 16,
    Minute = 17,
    Second = 18,
    Time = 19,
};

/// The q type character (as shown by `meta`), such as `j` for long.
char qTypeChar(QType type);

/// An atom of a literal.
///
/// The value is held as follows: integers and booleans as `Int64` (booleans 0 or 1), floats as `Float64`,
/// chars and symbols as `String`, temporal values as `Int64` counts of their natural unit:
/// timestamps and timespans in nanoseconds, months since 2000.01, dates in days since 2000.01.01,
/// datetimes in milliseconds since 2000.01.01, minutes, seconds, and times (milliseconds) since midnight.
struct QAtom
{
    Field value;
    bool is_null = false;
    /// +1 or -1 for the infinities `0W` and `-0W`.
    int8_t infinity = 0;
};

enum class QNodeKind : uint8_t
{
    /// An atom or a simple list of atoms of one type, like `42`, `1 2 3`, `` `a`b ``, `"abc"`.
    Literal,
    /// A name, like `x`, `t.col`, `.z.p`.
    Name,
    /// A primitive verb, like `+`, `<=`, `#`, `-:`.
    Verb,
    /// A function modified by an adverb: `+/`, `f'`, `,\:`. `name` holds the adverb, `children[0]` the operand.
    Derived,
    /// An application of `children[0]` to the arguments `children[1..]`.
    /// An elided argument of a projection (`f[;2]`) is `nullptr`.
    Apply,
    /// A general list: `(a;b;c)`, or `()` for the empty list.
    List,
    /// A lambda, like `{x+y}` or `{[a;b] a*b}`. `params` holds the parameter names, `children` the statements of the body.
    Lambda,
    /// An assignment `name:value` (or `name::value`). `children[0]` is the value.
    Assign,
    /// A q-sql template: `select`, `exec`, `update`, or `delete`.
    SQL,
    /// A table literal: `([] a:1 2; b:3 4)` or a keyed table literal `([k:1 2] v:3 4)`.
    Table,
};

struct QNode;
using QNodePtr = std::shared_ptr<const QNode>;

/// A named expression: a column of a q-sql template or of a table literal. The name is empty when not given.
struct QColumn
{
    String name;
    QNodePtr expression;
};

using QColumns = std::vector<QColumn>;

struct QSQLTemplate
{
    enum class Kind : uint8_t
    {
        Select,
        Exec,
        Update,
        Delete,
    };

    Kind kind = Kind::Select;
    bool distinct = false;

    /// `select[n]`: the number of rows to take (negative for the last rows), or a pair of the offset and the count.
    QNodePtr limit;

    /// `select[>col]` (descending) or its ascending counterpart: the sort column (an empty name for none) and the direction.
    String order_column;
    bool order_descending = false;

    /// The select phrase. For `delete`, the names of the columns to delete.
    QColumns columns;
    /// The by phrase.
    QColumns by;
    /// Whether the by phrase is present (`select by x from t` has a by phrase but no columns).
    bool has_by = false;

    QNodePtr from;
    /// The where phrase: the constraints are applied one after another.
    std::vector<QNodePtr> where;
};

struct QNode
{
    QNodeKind kind;
    /// The offset in the source text, for error messages.
    size_t offset = 0;

    /// Literal: the type of the atoms, and whether it is a list (a one-item list `enlist 1` is distinct from the atom `1`).
    QType type = QType::Mixed;
    bool is_list = false;
    std::vector<QAtom> atoms;

    /// Name: the name; Verb: the glyph; Derived: the adverb; Assign: the assigned name.
    String name;

    std::vector<QNodePtr> children;

    /// Apply: whether the application is written infix, as `x f y`.
    bool infix = false;
    /// Apply: whether the application is written by juxtaposition, as `f x`.
    bool juxtaposition = false;

    /// Lambda: the parameters, and the source text.
    std::vector<String> params;
    String source;

    /// SQL: the template.
    std::shared_ptr<const QSQLTemplate> sql;

    /// Table: the key columns and the value columns.
    QColumns key_columns;
    QColumns columns;

    explicit QNode(QNodeKind kind_, size_t offset_) : kind(kind_), offset(offset_) {}
};

/// A parsed q program: a sequence of statements. A `nullptr` statement is empty.
struct QProgram
{
    std::vector<QNodePtr> statements;
};

/// The dyadic keywords that can be used infix, like `x mod y` or `10 xbar t`.
bool isQInfixKeyword(std::string_view name);

}
