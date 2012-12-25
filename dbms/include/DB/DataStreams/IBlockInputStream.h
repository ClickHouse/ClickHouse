#pragma once

#include <Poco/SharedPtr.h>

#include <DB/Core/Block.h>


namespace DB
{

using Poco::SharedPtr;


/** Интерфейс потока для чтения данных по блокам из БД.
  * Реляционные операции предполагается делать также реализациями этого интерфейса.
  */
class IBlockInputStream
{
public:
	typedef SharedPtr<IBlockInputStream> BlockInputStreamPtr;
	typedef std::vector<BlockInputStreamPtr> BlockInputStreams;
	
	/** Прочитать следующий блок.
	  * Если блоков больше нет - вернуть пустой блок (для которого operator bool возвращает false).
	  */
	virtual Block read() = 0;

	/** Прочитать что-нибудь перед началом всех данных или после конца всех данных.
	  */
	virtual void readPrefix() {}
	virtual void readSuffix() {}

	virtual ~IBlockInputStream() {}

	/** Для вывода дерева преобразований потока данных (плана выполнения запроса).
	  */
	virtual String getName() const = 0;
	virtual String getShortName() const;	/// То же самое, но без BlockInputStream на конце.

	/** Создать копию объекта.
	  * Предполагается, что функция вызывается только до использования объекта (сразу после создания, до вызова других методов),
	  *  только для того, чтобы можно было преобразовать параметр, переданный по ссылке в shared ptr.
	  */
	virtual BlockInputStreamPtr clone() = 0;

	BlockInputStreams & getChildren() { return children; }
	
	void dumpTree(std::ostream & ostr, size_t indent = 0, size_t multiplier = 1);
	void dumpTreeWithProfile(std::ostream & ostr, size_t indent = 0);

	/// Получить листовые источники (не считая этот).
	BlockInputStreams getLeaves();

	/// Получить количество строк и байт, прочитанных в листовых источниках.
	void getLeafRowsBytes(size_t & rows, size_t & bytes);

	/** Получить текст, который идентифицирует этот источник и всё поддерево.
	  * Обычно он содержит идентификатор источника и getTreeID от всех детей.
	  */
	String getTreeID() const;

	/** Проверить глубину конвейера.
	  * Если задано max_depth и глубина больше - кинуть исключение.
	  */
	size_t checkDepth(size_t max_depth) const;

protected:
	BlockInputStreams children;

private:
	void getLeavesImpl(BlockInputStreams & res, BlockInputStreamPtr this_shared_ptr = NULL);

	size_t checkDepthImpl(size_t max_depth, size_t remaining_depth) const;
};


typedef IBlockInputStream::BlockInputStreamPtr BlockInputStreamPtr;
typedef IBlockInputStream::BlockInputStreams BlockInputStreams;

}

