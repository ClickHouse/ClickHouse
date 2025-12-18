#include <Common/XMLParsers/XPathEvaluator.h>

#include <Poco/DOM/Node.h>

#include <algorithm>
#include <cctype>
#include <sstream>


namespace DB
{

XPathEvaluator::NodeList XPathEvaluator::evaluate(const Node & context, const std::string & xpath)
{
    if (context.isNull() || xpath.empty())
        return {};

    auto steps = parseXPath(xpath);
    if (steps.empty())
        return {};

    NodeList current_nodes;
    current_nodes.push_back(context);

    for (const auto & step : steps)
        current_nodes = applyStep(current_nodes, step);

    return current_nodes;
}

XPathEvaluator::Node XPathEvaluator::evaluateToNode(const Node & context, const std::string & xpath)
{
    auto nodes = evaluate(context, xpath);
    if (nodes.empty())
        return Node(nullptr);
    return nodes[0];
}

std::string XPathEvaluator::evaluateToString(const Node & context, const std::string & xpath)
{
    auto node = evaluateToNode(context, xpath);
    if (node.isNull())
        return {};
    return node.innerText();
}

std::vector<XPathEvaluator::Step> XPathEvaluator::parseXPath(const std::string & xpath)
{
    std::vector<Step> steps;
    std::string path = trim(xpath);

    if (path.empty())
        return steps;

    /// Handle absolute path starting with /
    bool absolute = (path[0] == '/');
    if (absolute)
        path = path.substr(1);

    /// Handle // at the beginning (descendant-or-self)
    bool descendant_search = false;
    if (!path.empty() && path[0] == '/')
    {
        descendant_search = true;
        path = path.substr(1);
    }

    if (descendant_search)
    {
        /// Add a descendant-or-self::node() step
        Step dos_step;
        dos_step.axis = Axis::DescendantOrSelf;
        dos_step.node_test = "node()";
        steps.push_back(dos_step);
    }

    /// Split by '/' while respecting predicates
    std::vector<std::string> step_strings;
    std::string current_step;
    int bracket_depth = 0;

    for (size_t i = 0; i < path.size(); ++i)
    {
        char c = path[i];
        if (c == '[')
            ++bracket_depth;
        else if (c == ']')
            --bracket_depth;
        else if (c == '/' && bracket_depth == 0)
        {
            /// Check for // (descendant)
            if (i + 1 < path.size() && path[i + 1] == '/')
            {
                if (!current_step.empty())
                {
                    step_strings.push_back(current_step);
                    current_step.clear();
                }
                /// Add descendant-or-self::node()
                step_strings.push_back("descendant-or-self::node()");
                ++i;  /// Skip second /
                continue;
            }
            if (!current_step.empty())
            {
                step_strings.push_back(current_step);
                current_step.clear();
            }
            continue;
        }
        current_step += c;
    }
    if (!current_step.empty())
        step_strings.push_back(current_step);

    /// Parse each step
    for (const auto & step_str : step_strings)
        steps.push_back(parseStep(step_str));

    return steps;
}

XPathEvaluator::Step XPathEvaluator::parseStep(const std::string & step_str)
{
    Step step;
    std::string s = trim(step_str);

    /// Handle special steps
    if (s == ".")
    {
        step.axis = Axis::Self;
        step.node_test = "node()";
        return step;
    }
    if (s == "..")
    {
        step.axis = Axis::Parent;
        step.node_test = "node()";
        return step;
    }

    /// Handle descendant-or-self::node() (from //)
    if (s == "descendant-or-self::node()")
    {
        step.axis = Axis::DescendantOrSelf;
        step.node_test = "node()";
        return step;
    }

    /// Check for explicit axis
    auto axis_sep = s.find("::");
    if (axis_sep != std::string::npos)
    {
        std::string axis_name = s.substr(0, axis_sep);
        s = s.substr(axis_sep + 2);

        if (axis_name == "child")
            step.axis = Axis::Child;
        else if (axis_name == "descendant")
            step.axis = Axis::Descendant;
        else if (axis_name == "descendant-or-self")
            step.axis = Axis::DescendantOrSelf;
        else if (axis_name == "parent")
            step.axis = Axis::Parent;
        else if (axis_name == "self")
            step.axis = Axis::Self;
        else if (axis_name == "attribute")
            step.axis = Axis::Attribute;
        else
            step.axis = Axis::Child;  /// Default to child
    }
    else if (!s.empty() && s[0] == '@')
    {
        /// @attr shorthand for attribute::attr
        step.axis = Axis::Attribute;
        s = s.substr(1);
    }
    else
    {
        step.axis = Axis::Child;
    }

    /// Extract predicate if present
    auto pred_start = s.find('[');
    if (pred_start != std::string::npos)
    {
        auto pred_end = s.rfind(']');
        if (pred_end != std::string::npos && pred_end > pred_start)
        {
            step.predicate = s.substr(pred_start + 1, pred_end - pred_start - 1);
            s = s.substr(0, pred_start);
        }
    }

    step.node_test = trim(s);
    if (step.node_test.empty())
        step.node_test = "*";

    return step;
}

XPathEvaluator::NodeList XPathEvaluator::applyStep(const NodeList & nodes, const Step & step)
{
    NodeList result;

    for (const auto & node : nodes)
    {
        NodeList axis_nodes = applyAxis(node, step.axis);

        for (const auto & axis_node : axis_nodes)
        {
            if (matchesNodeTest(axis_node, step.node_test))
                result.push_back(axis_node);
        }
    }

    /// Apply predicate if present
    if (!step.predicate.empty())
        result = applyPredicate(result, step.predicate);

    return result;
}

XPathEvaluator::NodeList XPathEvaluator::applyAxis(const Node & node, Axis axis)
{
    NodeList result;

    switch (axis)
    {
        case Axis::Child:
            return node.children();

        case Axis::Descendant:
            getDescendants(node, result, false);
            return result;

        case Axis::DescendantOrSelf:
            getDescendants(node, result, true);
            return result;

        case Axis::Parent:
        {
            auto parent = node.parent();
            if (!parent.isNull())
                result.push_back(parent);
            return result;
        }

        case Axis::Self:
            result.push_back(node);
            return result;

        case Axis::Attribute:
        {
            /// Get attributes as nodes - handled specially in matchesNodeTest
            auto * raw_node = node.raw();
            if (raw_node && raw_node->nodeType() == Poco::XML::Node::ELEMENT_NODE)
            {
                auto * attrs = raw_node->attributes();
                if (attrs)
                {
                    for (unsigned long i = 0; i < attrs->length(); ++i)
                        result.emplace_back(attrs->item(i));
                    attrs->release();
                }
            }
            return result;
        }
    }

    return result;
}

bool XPathEvaluator::matchesNodeTest(const Node & node, const std::string & test)
{
    if (node.isNull())
        return false;

    /// Wildcard matches any element
    if (test == "*")
        return node.isElement();

    /// node() matches any node
    if (test == "node()")
        return true;

    /// text() matches text nodes
    if (test == "text()")
        return node.isText();

    /// Match by local name (namespace-stripped)
    std::string local_name = node.localName();
    std::string test_local = extractLocalName(test);

    return local_name == test_local;
}

XPathEvaluator::NodeList XPathEvaluator::applyPredicate(const NodeList & nodes, const std::string & predicate)
{
    if (nodes.empty())
        return nodes;

    std::string pred = trim(predicate);
    NodeList result;

    /// Check if predicate is a number (positional)
    bool is_number = true;
    for (char c : pred)
    {
        if (!std::isdigit(static_cast<unsigned char>(c)))
        {
            is_number = false;
            break;
        }
    }

    if (is_number && !pred.empty())
    {
        /// Positional predicate (1-indexed)
        size_t pos = static_cast<size_t>(std::stoul(pred));
        if (pos >= 1 && pos <= nodes.size())
            result.push_back(nodes[pos - 1]);
        return result;
    }

    /// Attribute predicate
    if (!pred.empty() && pred[0] == '@')
    {
        for (const auto & node : nodes)
        {
            if (evaluateAttributePredicate(node, pred))
                result.push_back(node);
        }
        return result;
    }

    /// Fallback: return all nodes (unsupported predicate)
    return nodes;
}

bool XPathEvaluator::evaluateAttributePredicate(const Node & node, const std::string & predicate)
{
    if (!node.isElement())
        return false;

    std::string pred = predicate;
    if (!pred.empty() && pred[0] == '@')
        pred = pred.substr(1);

    /// Check for = (attribute value comparison)
    auto eq_pos = pred.find('=');
    if (eq_pos != std::string::npos)
    {
        std::string attr_name = trim(pred.substr(0, eq_pos));
        std::string attr_value = trim(pred.substr(eq_pos + 1));

        /// Remove quotes from value
        if (attr_value.size() >= 2)
        {
            if ((attr_value.front() == '\'' && attr_value.back() == '\'')
                || (attr_value.front() == '"' && attr_value.back() == '"'))
            {
                attr_value = attr_value.substr(1, attr_value.size() - 2);
            }
        }

        /// Strip namespace from attribute name
        attr_name = extractLocalName(attr_name);

        return node.getAttribute(attr_name) == attr_value;
    }
    else
    {
        /// Just check attribute existence
        std::string attr_name = extractLocalName(pred);
        return node.hasAttribute(attr_name);
    }
}

void XPathEvaluator::getDescendants(const Node & node, NodeList & result, bool include_self)
{
    if (node.isNull())
        return;

    if (include_self)
        result.push_back(node);

    for (auto child = node.firstChild(); !child.isNull(); child = child.nextSibling())
        getDescendants(child, result, true);
}

std::string XPathEvaluator::trim(const std::string & s)
{
    auto start = s.find_first_not_of(" \t\n\r");
    if (start == std::string::npos)
        return {};
    auto end = s.find_last_not_of(" \t\n\r");
    return s.substr(start, end - start + 1);
}

}
