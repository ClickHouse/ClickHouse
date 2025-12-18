#pragma once

#include <Common/XMLParsers/PocoXMLParser.h>

#include <string>
#include <vector>


namespace DB
{

/// XPath 1.0 evaluator for Poco XML DOM.
/// Supports namespace-stripping: matches elements by local name only,
/// so 'Envelope' matches 'soap:Envelope'.
///
/// Supported XPath features:
/// - Location paths: /, //, ., ..
/// - Node tests: nodename, *, text(), node()
/// - Predicates: [n], [@attr], [@attr='value']
/// - Axes: child::, descendant::, parent::, attribute::, self::, descendant-or-self::
///
/// Examples:
///   /root/child
///   //element
///   /root/child[@attr='value']
///   /root/child[1]
///   //Body (matches soap:Body)
class XPathEvaluator
{
public:
    using Node = PocoXMLParser::Node;
    using NodeList = std::vector<Node>;

    /// Evaluate XPath expression and return all matching nodes.
    NodeList evaluate(const Node & context, const std::string & xpath);

    /// Evaluate XPath expression and return the first matching node.
    /// Returns null node if no match.
    Node evaluateToNode(const Node & context, const std::string & xpath);

    /// Evaluate XPath expression and return text content of first match.
    /// Returns empty string if no match.
    std::string evaluateToString(const Node & context, const std::string & xpath);

private:
    /// XPath step type
    enum class Axis
    {
        Child,
        Descendant,
        DescendantOrSelf,
        Parent,
        Self,
        Attribute
    };

    /// Represents a single step in a location path
    struct Step
    {
        Axis axis = Axis::Child;
        std::string node_test;      /// Element name, '*', 'text()', 'node()'
        std::string predicate;      /// Content inside [], empty if none
    };

    /// Parse XPath into steps
    std::vector<Step> parseXPath(const std::string & xpath);

    /// Parse a single step like 'child::element[predicate]'
    Step parseStep(const std::string & step_str);

    /// Apply a step to a list of nodes
    NodeList applyStep(const NodeList & nodes, const Step & step);

    /// Apply axis to a single node
    NodeList applyAxis(const Node & node, Axis axis);

    /// Check if node matches the node test (with namespace stripping)
    bool matchesNodeTest(const Node & node, const std::string & test);

    /// Apply predicate filter to node list
    NodeList applyPredicate(const NodeList & nodes, const std::string & predicate);

    /// Parse and evaluate attribute predicate like [@attr] or [@attr='value']
    bool evaluateAttributePredicate(const Node & node, const std::string & predicate);

    /// Get all descendant nodes
    void getDescendants(const Node & node, NodeList & result, bool include_self);

    /// Trim whitespace from string
    static std::string trim(const std::string & s);
};

}
