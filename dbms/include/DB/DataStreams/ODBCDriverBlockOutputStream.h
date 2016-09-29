#pragma once

#include <DB/DataStreams/IBlockOutputStream.h>


namespace DB
{

class WriteBuffer;


/** Формат данных, предназначенный для упрощения реализации ODBC драйвера.
  * ODBC драйвер предназначен для сборки под разные платформы без зависимостей от основного кода,
  *  поэтому формат сделан так, чтобы в нём можно было как можно проще его распарсить.
  * Выводится заголовок с нужной информацией.
  * Затем данные выводятся в порядке строк. Каждое значение выводится так: длина в формате VarUInt, затем данные в текстовом виде.
  */
class ODBCDriverBlockOutputStream : public IBlockOutputStream
{
public:
	ODBCDriverBlockOutputStream(WriteBuffer & out_);

	void write(const Block & block) override;

	void flush() override;
	String getContentType() const override { return "application/octet-stream"; }

private:
	bool is_first = true;
	WriteBuffer & out;
};

}
