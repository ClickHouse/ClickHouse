#pragma once
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>

#include <Parsers/IAST.h>
#include <re2/re2.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_REGEXP;
}

struct AsteriskSemantic;
struct AsteriskSemanticImpl;


class ASTColumnsClause : public IAST
{
public:

    String getID(char) const override { return "ColumnsClause"; }
    ASTPtr clone() const override;
    void appendColumnName(WriteBuffer & ostr) const override;
    void setPattern(String pattern)
    {
        originalPattern = pattern;
        columnMatcher = std::make_shared<RE2>(pattern, RE2::Quiet);
        if (!columnMatcher->ok())
            throw DB::Exception("COLUMNS pattern " + originalPattern + " cannot be compiled: " + columnMatcher->error(), DB::ErrorCodes::CANNOT_COMPILE_REGEXP);
    }
    bool isColumnMatching(String columnName) const
    {
        return RE2::FullMatch(columnName, *columnMatcher);
    }

protected:
    void formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;

private:
    std::shared_ptr<RE2> columnMatcher;
    String originalPattern;
    std::shared_ptr<AsteriskSemanticImpl> semantic; /// pimpl

    friend struct AsteriskSemantic;
};


}
