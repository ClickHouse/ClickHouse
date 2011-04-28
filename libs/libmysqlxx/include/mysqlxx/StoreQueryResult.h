#ifndef MYSQLXX_STOREQUERYRESULT_H
#define MYSQLXX_STOREQUERYRESULT_H

#include <vector>

#include <mysqlxx/ResultBase.h>
#include <mysqlxx/Row.h>


namespace mysqlxx
{

class Connection;


/** Результат выполнения запроса, загруженный полностью на клиента.
  * Это требует оперативку, чтобы вместить весь результат,
  *  но зато реализует произвольный доступ к строкам по индексу.
  * Если размер результата большой - используйте лучше UseQueryResult.
  * Объект содержит ссылку на Connection.
  * Если уничтожить Connection, то объект становится некорректным и все строки результата - тоже.
  * Если задать следующий запрос в соединении, то объект и все строки тоже становятся некорректными.
  */
class StoreQueryResult : public std::vector<Row>, public ResultBase
{
public:
	StoreQueryResult(MYSQL_RES * res_, Connection * conn_, const Query * query_);

	StoreQueryResult(const StoreQueryResult & x);
	StoreQueryResult & operator= (const StoreQueryResult & x);

	size_t num_rows() const { return size(); }

private:

	/** Не смотря на то, что весь результат выполнения запроса загружается на клиента,
	  *  и все указатели MYSQL_ROW на отдельные строки различные,
	  *  при этом функция mysql_fetch_lengths() возвращает длины
	  *  для текущей строки по одному и тому же адресу.
	  * То есть, чтобы можно было пользоваться несколькими Row одновременно,
	  *  необходимо заранее куда-то сложить все длины.
	  */
	typedef std::vector<MYSQL_LENGTH> Lengths;
	Lengths lengths;

	void init();
};

}

#endif
