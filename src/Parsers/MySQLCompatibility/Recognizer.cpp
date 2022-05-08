#include <Parsers/MySQLCompatibility/Recognizer.h>

#include <Parsers/MySQLCompatibility/DescribeCommandCT.h>
#include <Parsers/MySQLCompatibility/SelectQueryCT.h>
#include <Parsers/MySQLCompatibility/SetQueryCT.h>
#include <Parsers/MySQLCompatibility/ShowQueryCT.h>
#include <Parsers/MySQLCompatibility/UseCommandCT.h>

namespace MySQLCompatibility
{

ConvPtr SetQueryRecognizer::Recognize(MySQLPtr node) const
{
    if (node->rule_name == "setStatement")
        return std::make_shared<SetQueryCT>(node);
    return nullptr;
}

ConvPtr SelectQueryRecognizer::Recognize(MySQLPtr node) const
{
    if (node->rule_name == "selectStatement")
        return std::make_shared<SelectQueryCT>(node);

    return nullptr;
}

ConvPtr UseCommandRecognizer::Recognize(MySQLPtr node) const
{
    if (node->rule_name == "useCommand")
        return std::make_shared<UseCommandCT>(node);

    return nullptr;
}

ConvPtr ShowQueryRecognizer::Recognize(MySQLPtr node) const
{
    if (node->rule_name == "showStatement")
        return std::make_shared<ShowQueryCT>(node);

    return nullptr;
}

ConvPtr DescribeCommandRecognizer::Recognize(MySQLPtr node) const
{
    if (node->rule_name == "describeCommand")
        return std::make_shared<DescribeCommandCT>(node);

    return nullptr;
}

ConvPtr GenericRecognizer::Recognize(MySQLPtr node) const
{
    ConvPtr result = nullptr;
    for (const auto & rule : rules)
    {
        if ((result = rule->Recognize(node)) != nullptr)
            return result;
    }

    for (auto child : node->children)
    {
        if ((result = this->Recognize(child)) != nullptr)
            return result;
    }

    return result;
}
}
