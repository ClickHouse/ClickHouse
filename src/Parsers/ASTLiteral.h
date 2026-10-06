#pragma once

#include <Core/Field.h>
#include <DataTypes/IDataType.h>
#include <Parsers/ASTWithAlias.h>

namespace DB
{

/// Literal (atomic) - number, string, NULL
class ASTLiteral : public ASTWithAlias
{
protected:
    struct ASTLiteralFlags
    {
        using ParentFlags = ASTWithAliasFlags;
        static constexpr UInt32 RESERVED_BITS = ASTWithAliasFlags::RESERVED_BITS + 1;

        UInt32 _parent_reserved : ParentFlags::RESERVED_BITS;
        UInt32 use_legacy_column_name_of_tuple : 1;
        UInt32 unused : 30;
    };

public:
    explicit ASTLiteral(Field value_)
        : value(std::move(value_))
    {
    }

    Field value;

    /*
     * The name of the column corresponding to this literal. Only used to
     * disambiguate the literal columns with the same display name that are
     * created at the expression analyzer stage. In the future, we might want to
     * have a full separation between display names and column identifiers. For
     * now, this field is effectively just some private EA data.
     */
    String unique_column_name;

    void setUseLegacyColumnNameOfTuple(bool _value)
    {
        flags<ASTLiteralFlags>().use_legacy_column_name_of_tuple = _value;
    }

    bool getUseLegacyColumnNameOfTuple() const
    {
        return flags<ASTLiteralFlags>().use_legacy_column_name_of_tuple;
    }

    /** Get the text that identifies this element. */
    String getID(char delim) const override;

    ASTPtr clone() const override;

    void updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const override;

protected:
    void formatImplWithoutAlias(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

    void appendColumnNameImpl(WriteBuffer & ostr) const override;

private:
    /// Legacy version of 'appendColumnNameImpl'. It differs only with tuple literals.
    /// It's only needed to continue working of queries with tuple literals
    /// in distributed tables while rolling update.
    void appendColumnNameImplLegacy(WriteBuffer & ostr) const;
};

}
