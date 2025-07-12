#pragma once

#include <Functions/XPath/Instructions/InstructionSelectNth.h>
#include <Functions/XPath/Instructions/InstructionSelectTagsWithAttributes.h>
#include <Functions/XPath/Instructions/InstructionText.h>
#include <Functions/XPath/Types.h>

#include <Functions/XPath/ASTs/ASTXPath.h>
#include <Functions/XPath/ASTs/ASTXPathAttribute.h>
#include <Functions/XPath/ASTs/ASTXPathIndexAccess.h>
#include <Functions/XPath/ASTs/ASTXPathMemberAccess.h>
#include <Functions/XPath/ASTs/ASTXPathText.h>
#include <Functions/XPath/ValueMatchers.h>

#include <Common/TypePromotion.h>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

inline std::vector<InstructionPtr> getInstructionsFromAST(ASTPtr query_ptr)
{
    std::vector<InstructionPtr> instructions;
    const auto * path = query_ptr->as<ASTXPath>();

    if (!path)
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Invalid path");
    }

    const auto * query = path->xpath_query;
    std::string_view last_requeted_tag;
    bool last_tag_can_skip_ancestors = false;
    std::vector<TagMatcher> tag_matchers{};
    std::vector<AttributeMatcher> attribute_matchers{};
    bool need_add_member_access_instruction = false;

    for (const auto & child_ast : query->children)
    {
        if (typeid_cast<ASTXPathMemberAccess *>(child_ast.get()))
        {
            const auto * member_access_ast = child_ast->as<ASTXPathMemberAccess>();

            if (need_add_member_access_instruction)
            {
                tag_matchers.emplace_back(
                    CaseInsensitiveStringView(last_requeted_tag), std::move(attribute_matchers), last_tag_can_skip_ancestors);
                attribute_matchers = std::vector<AttributeMatcher>{};

                if (member_access_ast->skip_ancestors)
                {
                    instructions.push_back(std::make_shared<InstructionSelectTagsWithAttributes>(std::move(tag_matchers)));
                    tag_matchers = std::vector<TagMatcher>{};
                }
            }

            last_requeted_tag = member_access_ast->member_name;
            last_tag_can_skip_ancestors = member_access_ast->skip_ancestors;
            need_add_member_access_instruction = true;
        }
        else if (typeid_cast<ASTXPathIndexAccess *>(child_ast.get()))
        {
            const auto * index_access_ast = child_ast->as<ASTXPathIndexAccess>();

            if (need_add_member_access_instruction)
            {
                tag_matchers.emplace_back(CaseInsensitiveStringView(last_requeted_tag), std::move(attribute_matchers));
                instructions.push_back(std::make_shared<InstructionSelectTagsWithAttributes>(std::move(tag_matchers)));

                attribute_matchers = std::vector<AttributeMatcher>{};
                tag_matchers = std::vector<TagMatcher>{};
                need_add_member_access_instruction = false;
            }
            instructions.push_back(std::make_shared<InstructionSelectNth>(index_access_ast->index));
        }
        else if (typeid_cast<ASTXPathAttribute *>(child_ast.get()))
        {
            const auto * attribute_ast = child_ast->as<ASTXPathAttribute>();

            const auto & attribute_name = attribute_ast->name;
            const auto & attribute_value = attribute_ast->value;
            ValueMatcherPtr value_matcher;

            if (attribute_value.empty())
            {
                value_matcher = MakeAlwaysTrueMatcher();
            }
            else
            {
                value_matcher = MakeExactMatcher(attribute_value);
            }

            attribute_matchers.emplace_back(attribute_name, std::move(value_matcher));
            need_add_member_access_instruction = true;
        }
        else if (typeid_cast<ASTXPathText *>(child_ast.get()))
        {
            // strange case, but sanity check
            if (need_add_member_access_instruction)
            {
                tag_matchers.emplace_back(CaseInsensitiveStringView(last_requeted_tag), std::move(attribute_matchers));
                instructions.push_back(std::make_shared<InstructionSelectTagsWithAttributes>(std::move(tag_matchers)));

                attribute_matchers = std::vector<AttributeMatcher>{};
                tag_matchers = std::vector<TagMatcher>{};
                need_add_member_access_instruction = false;
            }

            instructions.push_back(std::make_shared<InstructionText>());
        }
    }

    if (need_add_member_access_instruction)
    {
        tag_matchers.emplace_back(CaseInsensitiveStringView(last_requeted_tag), std::move(attribute_matchers), last_tag_can_skip_ancestors);
        instructions.push_back(std::make_shared<InstructionSelectTagsWithAttributes>(std::move(tag_matchers)));
    }

    return instructions;
}
}
