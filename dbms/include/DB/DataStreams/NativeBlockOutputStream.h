#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

/** Сериализует поток блоков в родном бинарном формате (с именами и типами столбцов).
  * Предназначено для взаимодействия между серверами.
  */
class NativeBlockOutputStream : public IBlockOutputStream
{
public:
	/** В случае указания ненулевой client_revision, может записываться дополнительная информация о блоке,
	  *  в зависимости от поддерживаемой для указанной ревизии.
	  */
	NativeBlockOutputStream(WriteBuffer & ostr_, UInt64 client_revision_ = 0)
		: ostr(ostr_), client_revision(client_revision_) {}

	void write(const Block & block) override;
	void flush() override { ostr.next(); }

	static void writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit);

private:
	WriteBuffer & ostr;
	UInt64 client_revision;
};

}
