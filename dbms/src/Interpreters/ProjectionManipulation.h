#pragma once

#include <string>
#include <Interpreters/ExpressionAnalyzer.h>

namespace DB
{
class ExpressionAnalyzer;

struct ScopeStack;

namespace ErrorCodes
{
    extern const int CONDITIONAL_TREE_PARENT_NOT_FOUND;
    extern const int ILLEGAL_PROJECTION_MANIPULATOR;
}

/*
 * This is a base class for the ConditionalTree. Look at the description of ConditionalTree.
 */
struct ProjectionManipulatorBase
{
public:
    virtual bool tryToGetFromUpperProjection(const std::string & column_name) = 0;

    virtual std::string getColumnName(const std::string & col_name) const = 0;

    virtual std::string getProjectionExpression() = 0;

    virtual std::string getProjectionSourceColumn() const = 0;

    virtual ~ProjectionManipulatorBase();
};

using ProjectionManipulatorPtr = std::shared_ptr<ProjectionManipulatorBase>;

/*
 * This is the default ProjectionManipulator. It is needed for backwards compatibility.
 * For the better understanding of what ProjectionManipulator does,
 * look at the description of ConditionalTree.
 */
struct DefaultProjectionManipulator : public ProjectionManipulatorBase
{
private:
    ScopeStack & scopes;

public:
    explicit DefaultProjectionManipulator(ScopeStack & scopes);

    bool tryToGetFromUpperProjection(const std::string & column_name) final;

    std::string getColumnName(const std::string & col_name) const final;

    std::string getProjectionExpression() final;

    std::string getProjectionSourceColumn() const final;
};

/*
 * ConditionalTree is a projection manipulator. It is used in ExpressionAnalyzer::getActionsImpl.
 * It is a helper class, which helps to build sequence of ExpressionAction instances -- actions, needed for
 * computation of expression. It represents the current state of a projection layer. That is, if we have an expression
 * f and g, we need to calculate f, afterwards we need to calculate g on the projection layer <f != 0>.
 * This projection layer is stored in the ConditionalTree. Also, it stores the tree of all projection layers, which
 * was seen before. If we have seen the projection layer <f != 0> and <f != 0 and g != 0>, conditional tree will put
 * the second layer as a child to the first one.
 *
 * The description of what methods do:
 *   1) getColumnName -- constructs the name of expression. which contains the information of the projection layer.
 *   It is needed to make computed column name unique. That is, if we have an expression g and conditional layer
 *   <f != 0>, it forms the name g<f != 0>
 *
 *   2) goToProjection -- accepts field name f and builds child projection layer with the additional condition
 *   <f>. For instance, if we are on the projection layer a != 0 and the function accepts the expression b != 0,
 *   it will build a projection layer <a != 0 and b != 0>, and remember that this layer is a child to a previous one.
 *   Moreover, the function will store the actions to build projection between this two layers in the corresponding
 *   ScopeStack
 *
 *   3) restoreColumn(default_values_name, new_values_name, levels, result_name) -- stores action to restore calculated
 *   'new_values_name' column, to insert its values to the projection layer, which is 'levels' number of levels higher.
 *
 *   4) goUp -- goes several levels up in the conditional tree, raises the exception if we hit the root of the tree and
 *   there are still remained some levels up to go.
 *
 *   5) tryToGetFromUpperProjection -- goes up to the root projection level and checks whether the expression is
 *   already calculated somewhere in the higher projection level. If it is, we may just project it to the current
 *   layer to have it computed in the current layer. In this case, the function stores all actions needed to compute
 *   the projection: computes composition of projections and uses it to project the column. In the other case, if
 *   the column is not computed on the higher level, the function returns false. It is used in getActinosImpl to
 *   understand whether we need to scan the expression deeply, or can it be easily computed just with the projection
 *   from one of the higher projection layers.
 */
struct ConditionalTree : public ProjectionManipulatorBase
{
private:
    struct Node
    {
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

    std::string getProjectionColumnName(const std::string & first_projection_expr, const std::string & second_projection_expr) const;

    std::string getProjectionColumnName(size_t first_index, size_t second_index) const;

    void buildProjectionCompositionRecursive(const std::vector<size_t> & path, size_t child_index, size_t parent_index);

    void buildProjectionComposition(size_t child_node, size_t parent_node);

    std::string getProjectionSourceColumn(size_t node) const;

public:
    ConditionalTree(ScopeStack & scopes, const Context & context);

    std::string getColumnName(const std::string & col_name) const final;

    void goToProjection(const std::string & field_name);

    std::string buildRestoreProjectionAndGetName(size_t levels_up);

    void restoreColumn(
        const std::string & default_values_name, const std::string & new_values_name, size_t levels_up, const std::string & result_name);

    void goUp(size_t levels_up);

    bool tryToGetFromUpperProjection(const std::string & column_name) final;

    std::string getProjectionExpression() final;

    std::string getProjectionSourceColumn() const final;
};

using ConditionalTreePtr = std::shared_ptr<ConditionalTree>;

/*
 * ProjectionAction describes in what way should some specific function use the projection manipulator.
 * This class has two inherited classes: DefaultProjectionAction, which does nothing, and AndOperatorProjectionAction,
 * which represents how function "and" uses projection manipulator.
 */
class ProjectionActionBase
{
public:
    /*
     * What to do before scanning the function argument (each of it)
     */
    virtual void preArgumentAction() = 0;

    /*
     * What to do after scanning each argument
     */
    virtual void postArgumentAction(const std::string & argument_name) = 0;

    /*
     * What to do after scanning all the arguments, before the computation
     */
    virtual void preCalculation() = 0;

    /*
     * Should default computation procedure be run or not
     */
    virtual bool isCalculationRequired() = 0;

    virtual ~ProjectionActionBase();
};

using ProjectionActionPtr = std::shared_ptr<ProjectionActionBase>;

class DefaultProjectionAction : public ProjectionActionBase
{
public:
    void preArgumentAction() final;

    void postArgumentAction(const std::string & argument_name) final;

    void preCalculation() final;

    bool isCalculationRequired() final;
};

/*
 * This is a specification of ProjectionAction specifically for the 'and' operation
 */
class AndOperatorProjectionAction : public ProjectionActionBase
{
private:
    ScopeStack & scopes;
    ProjectionManipulatorPtr projection_manipulator;
    std::string previous_argument_name;
    size_t projection_levels_count;
    std::string expression_name;
    const Context & context;

    std::string getZerosColumnName();

    std::string getFinalColumnName();

    void createZerosColumn(const std::string & restore_projection_name);

public:
    AndOperatorProjectionAction(
        ScopeStack & scopes, ProjectionManipulatorPtr projection_manipulator, const std::string & expression_name, const Context & context);

    /*
     * Before scanning each argument, we should go to the next projection layer. For example, if the expression is
     * f and g and h, then before computing g we should project to <f != 0> and before computing h we should project to
     * <f != 0 and g != 0>
     */
    void preArgumentAction() final;

    /*
     * Stores the previous argument name
     */
    void postArgumentAction(const std::string & argument_name) final;

    /*
     * Restores the result column to the uppermost projection level. For example, if the expression is f and g and h,
     * we should restore h<f,g> to the main projection layer
     */
    void preCalculation() final;

    /*
     * After what is done in preCalculation, we do not need to run default calculation of 'and' operator. So, the
     * function returns false.
     */
    bool isCalculationRequired() final;
};

/*
 * This function accepts the operator name and returns its projection action. For example, for 'and' operator,
 * it returns the pointer to AndOperatorProjectionAction.
 */
ProjectionActionPtr getProjectionAction(const std::string & node_name,
    ScopeStack & scopes,
    ProjectionManipulatorPtr projection_manipulator,
    const std::string & expression_name,
    const Context & context);

}
