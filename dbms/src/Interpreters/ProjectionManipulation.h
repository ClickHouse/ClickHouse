#pragma once

#include <string>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB {

class ExpressionAnalyzer;

class ScopeStack;

namespace ErrorCodes {
extern const int CONDITIONAL_TREE_PARENT_NOT_FOUND;
extern const int ILLEGAL_PROJECTION_MANIPULATOR;
}

struct ProjectionManipulatorBase {
public:
    virtual bool isAlreadyComputed(const std::string & column_name) = 0;

    virtual std::string getColumnName(const std::string & col_name) const = 0;

    virtual std::string getProjectionExpression() = 0;

    virtual ~ProjectionManipulatorBase();
};

using ProjectionManipulatorPtr = std::shared_ptr<ProjectionManipulatorBase>;

struct DefaultProjectionManipulator : public ProjectionManipulatorBase {
private:
    ScopeStack & scopes;
public:
    explicit DefaultProjectionManipulator(ScopeStack & scopes);

    bool isAlreadyComputed(const std::string & column_name) final;

    std::string getColumnName(const std::string & col_name) const final;

    std::string getProjectionExpression() final;
};

struct ConditionalTree : public ProjectionManipulatorBase {
private:
    struct Node {
        Node();

        size_t getParentNode() const;

        std::string projection_expression_string;
        size_t parent_node;
        bool is_root;
    };

    size_t current_node;
    std::vector<Node> nodes;
    ScopeStack & scopes;
    const Context & context;
    std::unordered_map<std::string, size_t> projection_expression_index;
private:
    std::string getColumnNameByIndex(const std::string & col_name, size_t node) const;

    std::string getProjectionColumnName(const std::string & first_projection_expr,
                                        const std::string & second_projection_expr);

    std::string getProjectionColumnName(size_t first_index, size_t second_index);

    void buildProjectionCompositionRecursive(const std::vector<size_t> & path,
                                             size_t child_index,
                                             size_t parent_index);

    void buildProjectionComposition(size_t child_node, size_t parent_node);

public:
    ConditionalTree(ScopeStack & scopes, const Context & context);

    std::string getColumnName(const std::string & col_name) const final;

    void goToProjection(const std::string & field_name);

    void restoreColumn(
        const std::string & inital_values_name,
        const std::string & new_values_name,
        size_t levels_up,
        const std::string & result_name
    );

    void goUp(size_t levels_up);

    bool isAlreadyComputed(const std::string & column_name) final;

    std::string getProjectionExpression() final;
};

using ConditionalTreePtr = std::shared_ptr<ConditionalTree>;

class ProjectionActionBase {
public:
    virtual void preArgumentAction() = 0;

    virtual void postArgumentAction(const std::string & argument_name) = 0;

    virtual void preCalculation() = 0;

    virtual bool isCalculationRequired() = 0;

    virtual ~ProjectionActionBase();
};

using ProjectionActionPtr = std::shared_ptr<ProjectionActionBase>;

class DefaultProjectionAction : public ProjectionActionBase {
public:
    void preArgumentAction() final;

    void postArgumentAction(const std::string & argument_name) final;

    void preCalculation() final;

    bool isCalculationRequired() final;
};

class AndOperatorProjectionAction : public ProjectionActionBase {
private:
    ScopeStack & scopes;
    ProjectionManipulatorPtr projection_manipulator;
    std::string previous_argument_name;
    size_t projection_levels_count;
    std::string expression_name;
    const Context & context;

    std::string getZerosColumnName();

    std::string getFinalColumnName();

    void createZerosColumn();
public:
    AndOperatorProjectionAction(ScopeStack & scopes,
                                ProjectionManipulatorPtr projection_manipulator,
                                const std::string & expression_name,
                                const Context& context);

    void preArgumentAction() final;

    void postArgumentAction(const std::string & argument_name) final;

    void preCalculation() final;

    bool isCalculationRequired() final;
};

ProjectionActionPtr getProjectionAction(const std::string & node_name,
                                        ScopeStack & scopes,
                                        ProjectionManipulatorPtr projection_manipulator,
                                        const std::string & expression_name,
                                        const Context & context);

}
