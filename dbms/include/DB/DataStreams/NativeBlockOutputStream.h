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
  * Если делается append в уже существующий файл, и нужно записать индекс, то укажите initial_size_of_file.
  */
class NativeBlockOutputStream : public IBlockOutputStream
{
public:
	/** В случае указания ненулевой client_revision, может записываться дополнительная информация о блоке,
	  *  в зависимости от поддерживаемой для указанной ревизии.
	  */
	NativeBlockOutputStream(
		WriteBuffer & ostr_, UInt64 client_revision_ = 0,
		WriteBuffer * index_ostr_ = nullptr, size_t initial_size_of_file_ = 0);

	void write(const Block & block) override;
	void flush() override { ostr.next(); }

	static void writeData(const IDataType & type, const ColumnPtr & column, WriteBuffer & ostr, size_t offset, size_t limit);

	String getContentType() const override { return "application/octet-stream"; }

private:
	WriteBuffer & ostr;
	UInt64 client_revision;

	WriteBuffer * index_ostr;
	size_t initial_size_of_file;	/// Начальный размер файла с данными, если делается append. Используется для индекса.
	/// Если требуется записывать индекс, то ostr обязан быть CompressedWriteBuffer.
	CompressedWriteBuffer * ostr_concrete = nullptr;
};

}
