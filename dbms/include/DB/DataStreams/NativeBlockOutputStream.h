#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

class WriteBuffer;
class CompressedWriteBuffer;


/** Сериализует поток блоков в родном бинарном формате (с именами и типами столбцов).
  * Предназначено для взаимодействия между серверами.
  *
  * Может быть указан поток для записи индекса. Индекс содержит смещения до каждого кусочка каждого столбца.
  */
class NativeBlockOutputStream : public IBlockOutputStream
{
public:
	/** В случае указания ненулевой client_revision, может записываться дополнительная информация о блоке,
	  *  в зависимости от поддерживаемой для указанной ревизии.
	  */
	NativeBlockOutputStream(
		WriteBuffer & ostr_, UInt64 client_revision_ = 0,
		WriteBuffer * index_ostr_ = nullptr);

	void write(const Block & block) override;
	void flush() override { ostr.next(); }

	static void writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit);

	String getContentType() const override { return "application/octet-stream"; }

private:
	WriteBuffer & ostr;
	UInt64 client_revision;

	WriteBuffer * index_ostr;
	/// Если требуется записывать индекс, то ostr обязан быть CompressedWriteBuffer.
	CompressedWriteBuffer * ostr_concrete = nullptr;
};

}
