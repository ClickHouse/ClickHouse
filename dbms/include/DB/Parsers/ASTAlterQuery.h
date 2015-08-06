#pragma once

#include <DB/Parsers/IAST.h>
#include <mysqlxx/Manip.h>


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
		ATTACH_PARTITION,
		FETCH_PARTITION,
		FREEZE_PARTITION,
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
		ASTPtr col_decl;

		/** В запросе ADD COLUMN здесь опционально хранится имя столбца, следующее после AFTER
		  * В запросе DROP здесь хранится имя столбца для удаления
		  */
		ASTPtr column;

		/** В запросе DROP PARTITION здесь хранится имя partition'а.
		  */
		ASTPtr partition;
		bool detach = false; /// true для DETACH PARTITION.

		bool part = false; /// true для ATTACH [UNREPLICATED] PART
		bool unreplicated = false; /// true для ATTACH UNREPLICATED, DROP UNREPLICATED ...

		/** Для FETCH PARTITION - путь в ZK к шарду, с которого скачивать партицию.
		  */
		String from;

		/// deep copy
		void clone(Parameters & p) const
		{
			p = *this;
			p.col_decl = col_decl->clone();
			p.column = column->clone();
			p.partition = partition->clone();
		}
	};
	typedef std::vector<Parameters> ParameterContainer;
	ParameterContainer parameters;
	String database;
	String table;


	void addParameters(const Parameters & params)
	{
		parameters.push_back(params);
		if (params.col_decl)
			children.push_back(params.col_decl);
		if (params.column)
			children.push_back(params.column);
		if (params.partition)
			children.push_back(params.partition);
	}


	ASTAlterQuery(StringRange range_ = StringRange()) : IAST(range_) {};

	/** Получить текст, который идентифицирует этот элемент. */
	String getID() const override { return ("AlterQuery_" + database + "_" + table); };

	ASTPtr clone() const override
	{
		ASTAlterQuery * res = new ASTAlterQuery(*this);
		for (ParameterContainer::size_type i = 0; i < parameters.size(); ++i)
		{
			parameters[i].clone(res->parameters[i]);
		}
		return res;
	}

protected:
	void formatImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override
	{
		frame.need_parens = false;

		std::string indent_str = settings.one_line ? "" : std::string(4 * frame.indent, ' ');

		settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ALTER TABLE " << (settings.hilite ? hilite_none : "");

		if (!table.empty())
		{
			if (!database.empty())
			{
				settings.ostr << indent_str << database;
				settings.ostr << ".";
			}
			settings.ostr << indent_str << table;
		}
		settings.ostr << settings.nl_or_ws;

		for (size_t i = 0; i < parameters.size(); ++i)
		{
			const ASTAlterQuery::Parameters & p = parameters[i];

			if (p.type == ASTAlterQuery::ADD_COLUMN)
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ADD COLUMN " << (settings.hilite ? hilite_none : "");
				p.col_decl->formatImpl(settings, state, frame);

				/// AFTER
				if (p.column)
				{
					settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << " AFTER " << (settings.hilite ? hilite_none : "");
					p.column->formatImpl(settings, state, frame);
				}
			}
			else if (p.type == ASTAlterQuery::DROP_COLUMN)
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "DROP COLUMN " << (settings.hilite ? hilite_none : "");
				p.column->formatImpl(settings, state, frame);
			}
			else if (p.type == ASTAlterQuery::MODIFY_COLUMN)
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "MODIFY COLUMN " << (settings.hilite ? hilite_none : "");
				p.col_decl->formatImpl(settings, state, frame);
			}
			else if (p.type == ASTAlterQuery::DROP_PARTITION)
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << (p.detach ? "DETACH" : "DROP") << " PARTITION "
				<< (settings.hilite ? hilite_none : "");
				p.partition->formatImpl(settings, state, frame);
			}
			else if (p.type == ASTAlterQuery::ATTACH_PARTITION)
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "ATTACH " << (p.unreplicated ? "UNREPLICATED " : "")
				<< (p.part ? "PART " : "PARTITION ") << (settings.hilite ? hilite_none : "");
				p.partition->formatImpl(settings, state, frame);
			}
			else if (p.type == ASTAlterQuery::FETCH_PARTITION)
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "FETCH " << (p.unreplicated ? "UNREPLICATED " : "")
				<< "PARTITION " << (settings.hilite ? hilite_none : "");
				p.partition->formatImpl(settings, state, frame);
				settings.ostr << (settings.hilite ? hilite_keyword : "")
					<< " FROM " << (settings.hilite ? hilite_none : "") << mysqlxx::quote << p.from;
			}
			else if (p.type == ASTAlterQuery::FREEZE_PARTITION)
			{
				settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << "FREEZE PARTITION " << (settings.hilite ? hilite_none : "");
				p.partition->formatImpl(settings, state, frame);
			}
			else
				throw Exception("Unexpected type of ALTER", ErrorCodes::UNEXPECTED_AST_STRUCTURE);

			std::string comma = (i < (parameters.size() -1) ) ? "," : "";
			settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str << comma << (settings.hilite ? hilite_none : "");

			settings.ostr << settings.nl_or_ws;
		}
	}
};

}
