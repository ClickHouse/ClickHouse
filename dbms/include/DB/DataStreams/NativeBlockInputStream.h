#pragma once

#include <DB/DataTypes/DataTypeFactory.h>
#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Десериализует поток блоков из родного бинарного формата (с именами и типами столбцов).
  * Предназначено для взаимодействия между серверами.
  */
class NativeBlockInputStream : public IProfilingBlockInputStream
{
public:
	/** В случае указания ненулевой server_revision, может ожидаться и считываться дополнительная информация о блоке,
	  * в зависимости от поддерживаемой для указанной ревизии.
	  */
	NativeBlockInputStream(ReadBuffer & istr_, const DataTypeFactory & data_type_factory_, UInt64 server_revision_ = 0)
		: istr(istr_), data_type_factory(data_type_factory_), server_revision(server_revision_) {}

	String getName() const override { return "NativeBlockInputStream"; }

	String getID() const override
	{
		std::stringstream res;
		res << this;
		return res.str();
	}

	static void readData(const IDataType & type, IColumn & column, ReadBuffer & istr, size_t rows);

protected:
	Block readImpl() override;

private:
	ReadBuffer & istr;
	const DataTypeFactory & data_type_factory;
	UInt64 server_revision;
};

}
