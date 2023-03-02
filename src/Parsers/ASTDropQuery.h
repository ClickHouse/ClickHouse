#pragma once

#include <Parsers/ASTQueryWithTableAndOutput.h>
#include <Parsers/ASTQueryWithOnCluster.h>


namespace DB
{

/** DROP query
  */
class ASTDropQuery : public ASTQueryWithTableAndOutput, public ASTQueryWithOnCluster
{
public:
    enum Kind
    {
        Drop,
        Detach,
        Truncate,
    };

    Kind kind;
    bool if_exists{false};

    /// Useful if we already have a DDL lock
    bool no_ddl_lock{false};

    /// We dropping dictionary, so print correct word
    bool is_dictionary{false};

    /// Same as above
    bool is_view{false};

    bool no_delay{false};

    // We detach the object permanently, so it will not be reattached back during server restart.
    bool permanently{false};

    /** Get the text that identifies this element. */
    String getID(char) const override;
    ASTPtr clone() const override;

    ASTPtr getRewrittenASTWithoutOnCluster(const std::string & new_database) const override
    {
        return removeOnCluster<ASTDropQuery>(clone(), new_database);
    }

    virtual QueryKind getQueryKind() const override { return QueryKind::Drop; }

protected:
    void formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const override;
};

}
