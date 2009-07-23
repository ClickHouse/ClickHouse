#ifndef DBMS_AGGREGATE_FUNCTION_H
#define DBMS_AGGREGATE_FUNCTION_H


namespace DB
{


/** Агрегатная функция
  * Шаблонный параметр здесь только потому что непонятно, как сделать forward declaration Field из Field.h.
  */
template<typename Field>
class IAggregateFunction
{
public:
	virtual void add(const Field & a) = 0;

	virtual Field getValue() const = 0;

	/** Бинарная сериализация и десериализация состояния для передачи по сети.
	  * При чём, deserializeMerge производит объединение текущего и считываемого состояний.
	  */
	virtual void serialize(std::ostream & ostr) const = 0;
	virtual void deserializeMerge(std::istream & istr) = 0;

	/** Объединение состояния агрегатной функции с состоянием другой агрегатной функции. */
	virtual void merge(const IAggregateFunction & rhs) = 0;

	virtual ~IAggregateFunction() {}
};


}

#endif
