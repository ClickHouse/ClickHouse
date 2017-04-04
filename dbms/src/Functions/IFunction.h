#pragma once

#include <memory>

#include <Core/Names.h>
#include <Core/Block.h>
#include <Core/ColumnNumbers.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <DataTypes/IDataType.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int ILLEGAL_COLUMN;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int FUNCTION_CANNOT_HAVE_PARAMETERS;
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
}

struct ExpressionAction;


/** Интерфейс для обычных функций.
  * Обычные функции - это функции, которые не меняют количество строк в таблице,
  *  и результат работы которых для каждой строчки не зависит от других строк.
  *
  * Функция может принимать произвольное количество аргументов; возвращает ровно одно значение.
  * Тип результата зависит от типов и количества аргументов.
  *
  * Функция диспетчеризуется для целого блока. Это позволяет производить всевозможные проверки редко,
  *  и делать основную работу в виде эффективного цикла.
  *
  * Функция применяется к одному или нескольким столбцам блока, и записывает свой результат,
  *  добавляя новый столбец к блоку. Функция не модифицирует свои агрументы.
  */
class IFunction
{
public:
    /** Наследник IFunction должен реализовать:
      * - getName
      * - либо getReturnType, либо getReturnTypeAndPrerequisites
      * - одну из перегрузок execute.
      */

    /// Получить основное имя функции.
    virtual String getName() const = 0;

    /// Override and return true if function could take different number of arguments.
    virtual bool isVariadic() const { return false; }

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t getNumberOfArguments() const = 0;

    /// Throw if number of arguments is incorrect. Default implementation will check only in non-variadic case.
    /// It is called inside getReturnType.
    virtual void checkNumberOfArguments(size_t number_of_arguments) const;

    /** Should we evaluate this function while constant folding, if arguments are constants?
      * Usually this is true. Notable counterexample is function 'sleep'.
      * If we will call it during query analysis, we will sleep extra amount of time.
      */
    virtual bool isSuitableForConstantFolding() const { return true; }

    /** Function is called "injective" if it returns different result for different values of arguments.
      * Example: hex, negate, tuple...
      *
      * Function could be injective with some arguments fixed to some constant values.
      * Examples:
      *  plus(const, x);
      *  multiply(const, x) where x is an integer and constant is not divisable by two;
      *  concat(x, 'const');
      *  concat(x, 'const', y) where const contain at least one non-numeric character;
      *  concat with FixedString
      *  dictGet... functions takes name of dictionary as its argument,
      *   and some dictionaries could be explicitly defined as injective.
      *
      * It could be used, for example, to remove useless function applications from GROUP BY.
      *
      * Sometimes, function is not really injective, but considered as injective, for purpose of query optimization.
      * For example, toString function is not injective for Float64 data type,
      *  as it returns 'nan' for many different representation of NaNs.
      * But we assume, that it is injective. This could be documented as implementation-specific behaviour.
      *
      * sample_block should contain data types of arguments and values of constants, if relevant.
      */
    virtual bool isInjective(const Block & sample_block) { return false; }

    /** Function is called "deterministic", if it returns same result for same values of arguments.
      * Most of functions are deterministic. Notable counterexample is rand().
      * Sometimes, functions are "deterministic" in scope of single query
      *  (even for distributed query), but not deterministic it general.
      * Example: now(). Another example: functions that work with periodically updated dictionaries.
      */
    virtual bool isDeterministicInScopeOfQuery() { return true; }

    /// Получить тип результата по типам аргументов. Если функция неприменима для данных аргументов - кинуть исключение.
    /// Перегрузка для тех, кому не нужны prerequisites и значения константных аргументов. Снаружи не вызывается.
    DataTypePtr getReturnType(const DataTypes & arguments) const;

    virtual DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const
    {
        throw Exception("getReturnType is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    /** Получить тип результата по типам аргументов и значениям константных аргументов.
      * Если функция неприменима для данных аргументов - кинуть исключение.
      * Еще можно вернуть описание дополнительных столбцов, которые требуются для выполнения функции.
      * Для неконстантных столбцов arguments[i].column = nullptr.
      * Осмысленные типы элементов в out_prerequisites: APPLY_FUNCTION, ADD_COLUMN.
      */
    void getReturnTypeAndPrerequisites(
        const ColumnsWithTypeAndName & arguments,
        DataTypePtr & out_return_type,
        std::vector<ExpressionAction> & out_prerequisites);

    virtual void getReturnTypeAndPrerequisitesImpl(
        const ColumnsWithTypeAndName & arguments,
        DataTypePtr & out_return_type,
        std::vector<ExpressionAction> & out_prerequisites)
    {
        DataTypes types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i)
            types[i] = arguments[i].type;
        out_return_type = getReturnTypeImpl(types);
    }

    /// For higher-order functions (functions, that have lambda expression as at least one argument).
    /// You pass data types with empty DataTypeExpression for lambda arguments.
    /// This function will replace it with DataTypeExpression containing actual types.
    void getLambdaArgumentTypes(DataTypes & arguments) const;

    virtual void getLambdaArgumentTypesImpl(DataTypes & arguments) const
    {
        throw Exception("Function " + getName() + " can't have lambda-expressions as arguments", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
    }

    /// Выполнить функцию над блоком. Замечание: может вызываться одновременно из нескольких потоков, для одного объекта.
    /// Перегрузка для тех, кому не нужны prerequisites. Снаружи не вызывается.
    void execute(Block & block, const ColumnNumbers & arguments, size_t result);

    /// Выполнить функцию над блоком. Замечание: может вызываться одновременно из нескольких потоков, для одного объекта.
    /// prerequisites идут в том же порядке, что и out_prerequisites, полученные из getReturnTypeAndPrerequisites.
    void execute(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & prerequisites, size_t result);

    virtual void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
    {
        throw Exception("executeImpl is not implemented for " + getName(), ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual void executeImpl(Block & block, const ColumnNumbers & arguments, const ColumnNumbers & prerequisites, size_t result)
    {
        executeImpl(block, arguments, result);
    }

    /// Returns true if the function implementation directly handles the arguments
    /// that correspond to nullable columns and null columns.
    virtual bool hasSpecialSupportForNulls() const { return false; }

    /** Позволяет узнать, является ли функция монотонной в некотором диапазоне значений.
      * Это используется для работы с индексом в сортированном куске данных.
      * И позволяет использовать индекс не только, когда написано, например date >= const, но и, например, toMonth(date) >= 11.
      * Всё это рассматривается только для функций одного аргумента.
      */
    virtual bool hasInformationAboutMonotonicity() const { return false; }

    /// Свойство монотонности на некотором диапазоне.
    struct Monotonicity
    {
        bool is_monotonic = false;    /// Является ли функция монотонной (неубывающей или невозрастающей).
        bool is_positive = true;    /// true, если функция неубывающая, false, если невозрастающая. Если is_monotonic = false, то не важно.

        Monotonicity(bool is_monotonic_ = false, bool is_positive_ = true)
            : is_monotonic(is_monotonic_), is_positive(is_positive_) {}
    };

    /** Получить информацию о монотонности на отрезке значений. Вызывайте только если hasInformationAboutMonotonicity.
      * В качестве одного из аргументов может быть передан NULL. Это значит, что соответствующий диапазон неограничен слева или справа.
      */
    virtual Monotonicity getMonotonicityForRange(const IDataType & type, const Field & left, const Field & right) const
    {
        throw Exception("Function " + getName() + " has no information about its monotonicity.", ErrorCodes::NOT_IMPLEMENTED);
    }

    virtual ~IFunction() {}

protected:
    /// Returns the copy of a given block in which each column specified in
    /// the "arguments" parameter is replaced with its respective nested
    /// column if it is nullable.
    static Block createBlockWithNestedColumns(const Block & block, ColumnNumbers args);
    /// Similar function as above. Additionally transform the result type if needed.
    static Block createBlockWithNestedColumns(const Block & block, ColumnNumbers args, size_t result);

private:
    /// Strategy to apply when executing a function.
    enum Strategy
    {
        /// Merely perform the function on its columns.
        DIRECTLY_EXECUTE = 0,
        /// If at least one argument is nullable, call the function implementation
        /// with a block in which nullable columns that correspond to function arguments
        /// have been replaced with their respective nested columns. Subsequently, the
        /// result column is wrapped into a nullable column.
        PROCESS_NULLABLE_COLUMNS,
        /// If at least one argument is NULL, return NULL.
        RETURN_NULL
    };

private:
    /// Choose the strategy for performing the function.
    Strategy chooseStrategy(const Block & block, const ColumnNumbers & args);

    /// If required by the specified strategy, process the given block, then
    /// return the processed block. Otherwise return an empty block.
    Block preProcessBlock(Strategy strategy, const Block & block, const ColumnNumbers & args,
        size_t result);

    /// If required by the specified strategy, post-process the result column.
    void postProcessResult(Strategy strategy, Block & block, const Block & processed_block,
        const ColumnNumbers & args, size_t result);
};


using FunctionPtr = std::shared_ptr<IFunction>;


}
