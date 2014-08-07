#pragma once

#include <DB/Parsers/IAST.h>

namespace DB
{

/** ALTER запрос
 *  ALTER TABLE [db.]name_type
 *  	ADD COLUMN col_name type [AFTER col_after],
 * 		DROP COLUMN col_drop,
 * 		MODIFY COLUMN col_name type,
 * 		DROP PARTITION partition
 * 		...
 */

class ASTAlterQuery : public IAST
{
public:
	enum ParameterType
	{
		ADD_COLUMN,
		DROP_COLUMN,
		MODIFY_COLUMN,
		DROP_PARTITION,
		NO_TYPE
	};

	struct Parameters
	{
		Parameters() : type(NO_TYPE) {}
		int type = NO_TYPE;

		/** В запросе ADD COLUMN здесь хранится имя и тип добавляемого столбца
		  *  В запросе DROP это поле не используется
		  *  В запросе MODIFY здесь хранится имя столбца и новый тип
		  */
		ASTPtr name_type;

		/** В запросе ADD COLUMN здесь опционально хранится имя столбца, следующее после AFTER
		  * В запросе DROP здесь хранится имя столбца для удаления
		  */
		ASTPtr column;

		/** В запросе DROP PARTITION здесь хранится имя partition'а.
		  */
		ASTPtr partition;
		bool detach = false; /// true для DETACH PARTITION.

		/// deep copy
		void clone(Parameters & p) const
		{
			p.type = type;
			p.name_type = name_type->clone();
			p.column = column->clone();
			p.partition = partition->clone();
			p.detach = detach;
		}
	};
	typedef std::vector<Parameters> ParameterContainer;
	ParameterContainer parameters;
	String database;
	String table;


	void addParameters(const Parameters & params)
	{
		parameters.push_back(params);
		if (params.name_type)
			children.push_back(params.name_type);
		if (params.column)
			children.push_back(params.column);
		if (params.partition)
			children.push_back(params.partition);
	}


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
