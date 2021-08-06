#include <Interpreters/RenameAliasesVisitor.h>
#include <Interpreters/IdentifierSemantic.h>

namespace DB
{

void RenameFunctionAliasesData::visit(ASTFunction & function, ASTPtr &) const
{
    int index;
    std::optional<String> alias_name = function.tryGetAlias();

    if (alias_name)
    {
        auto it = std::find(aliases_name.begin(), aliases_name.end(), alias_name);
        if (it != aliases_name.end())
        {
            index = it - aliases_name.begin();
            function.setAlias(renames_to.at(index));
        }
    }
}

}
