#pragma once

#include <list>
#include <sstream>

#include <Poco/SharedPtr.h>

#include <DB/Core/Types.h>
#include <DB/Parsers/StringRange.h>


namespace DB
{

using Poco::SharedPtr;


/** Элемент синтаксического дерева (в дальнейшем - направленного ациклического графа с элементами семантики)
  */
class IAST
{
public:
	typedef std::list<SharedPtr<IAST> > ASTs;
	ASTs children;
	StringRange range;
	/// Было ли соответствующее выражение вычислено.
	bool calculated;
	

	IAST() : range(NULL, NULL), calculated(false) {}
	IAST(StringRange range_) : range(range_), calculated(false) {}
	virtual ~IAST() {}
		
	/** Получить текст, который идентифицирует этот элемент. */
	virtual String getID() = 0;

	/** Получить текст, который идентифицирует этот элемент и всё поддерево.
	  * Обычно он содержит идентификатор элемента и getTreeID от всех детей. 
	  */
	String getTreeID()
	{
		std::stringstream s;
		s << getID();

		if (!children.empty())
		{
			s << "(";
			for (ASTs::iterator it = children.begin(); it != children.end(); ++it)
			{
				if (it != children.begin())
					s << ", ";
				s << (*it)->getTreeID();
			}
			s << ")";
		}

		return s.str();
	}
};


typedef SharedPtr<IAST> ASTPtr;
typedef std::list<ASTPtr> ASTs;

}
