#include <mysqlxx/ResultBase.h>

#include <iostream>

namespace mysqlxx
{

ResultBase::ResultBase(MYSQL_RES & res_, Connection & conn_) : res(res_), conn(conn_)
{
	fields = mysql_fetch_fields(&res);
	num_fields = mysql_num_fields(&res);

	std::cerr << num_fields << std::endl;
	std::cerr << fields[0].name << std::endl;
}

}
