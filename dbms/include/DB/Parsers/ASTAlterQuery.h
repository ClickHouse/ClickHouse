#pragma once

#include <DB/Parsers/IAST.h>
#include <boost/concept_check.hpp>

namespace DB
{

/** ALTER запрос
 *  ALTER TABLE [db.]name_type
 *  	ADD COLUMN col_name type [AFTER col_after],
 * 		DROP COLUMN col_drop,
 * 		MODIFY COLUMN col_name type
 * 		...
 */

class ASTAlterQuery : public IAST
{
public:
	enum ParameterType
	{
		ADD,
		DROP,
		MODIFY,
		NO_TYPE
	};

	struct Parameters
	{
		Parameters() : type(NO_TYPE) {}
		int type;

		/** В запросе ADD COLUMN здесь хранится имя и тип добавляемого столбца
		 *  В запросе DROP это поле не используется
		 *  В запросе MODIFY здесь хранится имя столбца и новый тип
		 */
		ASTPtr name_type;
		/** В запросе ADD COLUMN здесь опционально хранится имя столбца, следующее после AFTER
		 * В запросе DROP здесь хранится имя столбца для удаления
		 */
		ASTPtr column;

		/// deep copy
		void clone(Parameters & p) const
		{
			p.type = type;
			p.column = column->clone();
			p.name_type = name_type->clone();
		}
	};
	typedef std::vector<Parameters> ParameterContainer;
	ParameterContainer parameters;
	String database;
	String table;


	ASTAlterQuery(StringRange range_ = StringRange()) : IAST(range_) {};

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const { return ("AlterQuery_" + database + "_" + table); };

	ASTPtr clone() const
	{
		ASTAlterQuery * res = new ASTAlterQuery(*this);
		for (ParameterContainer::size_type i = 0; i < parameters.size(); ++i)
		{
			parameters[i].clone(res->parameters[i]);
		}
		return res;
	}
};
}
