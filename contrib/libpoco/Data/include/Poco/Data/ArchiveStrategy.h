//
// ArchiveStrategy.h
//
// $Id: //poco/Main/Data/include/Poco/ArchiveStrategy.h#1 $
//
// Library: Data
// Package: Logging
// Module:  ArchiveStrategy
//
// Definition of the ArchiveStrategy class and subclasses.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_ArchiveStrategy_INCLUDED
#define Data_ArchiveStrategy_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/Session.h"
#include "Poco/DateTime.h"
#include "Poco/Timespan.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/SharedPtr.h"


namespace Poco {
namespace Data {


class Data_API ArchiveStrategy
	/// The ArchiveStrategy is used by SQLChannel to archive log rows.
{
public:
	static const std::string DEFAULT_ARCHIVE_DESTINATION;

	ArchiveStrategy(const std::string& connector, 
		const std::string& connect, 
		const std::string& source, 
		const std::string& destination = DEFAULT_ARCHIVE_DESTINATION);
		/// Creates archive strategy.

	virtual ~ArchiveStrategy();
		/// Destroys archive strategy.

	void open();
		/// Opens the session.

	virtual void archive() = 0;
		/// Archives the rows.

	const std::string& getSource() const;
		/// Returns the name of the source table containing rows to be archived.

	void setSource(const std::string& source);
		/// Sets the name of the source table.

	const std::string& getDestination() const;
		/// Returns the name of the destination table for rows to be archived.

	void setDestination(const std::string& destination);
		/// Sets the name of the destination table.

	virtual const std::string& getThreshold() const = 0;
		/// Returns the archive threshold.

	virtual void setThreshold(const std::string& threshold) = 0;
		/// Sets the archive threshold.

protected:
	typedef Poco::SharedPtr<Session>   SessionPtr;
	typedef Poco::SharedPtr<Statement> StatementPtr;

	Session& session();

	void setCopyStatement();
	void setDeleteStatement();
	void setCountStatement();

	Statement& getCopyStatement();
	Statement& getDeleteStatement();
	Statement& getCountStatement();
private:
	
	ArchiveStrategy();
	ArchiveStrategy(const ArchiveStrategy&);
	ArchiveStrategy& operator = (const ArchiveStrategy&);

	std::string  _connector;
	std::string  _connect;
	SessionPtr   _pSession;
	StatementPtr _pCopyStatement;
	StatementPtr _pDeleteStatement;
	StatementPtr _pCountStatement;
	std::string  _source;
	std::string  _destination;
};


//
// inlines
//

inline const std::string& ArchiveStrategy::getSource() const
{
	return _source;
}


inline void ArchiveStrategy::setSource(const std::string& source)
{
	_source = source;
}


inline void ArchiveStrategy::setDestination(const std::string& destination)
{
	_destination = destination;
}


inline const std::string& ArchiveStrategy::getDestination() const
{
	return _destination;
}


inline Session& ArchiveStrategy::session()
{
	return *_pSession;
}


inline void ArchiveStrategy::setCopyStatement()
{
	_pCopyStatement = new Statement(*_pSession);
}


inline void ArchiveStrategy::setDeleteStatement()
{
	_pDeleteStatement = new Statement(*_pSession);
}


inline void ArchiveStrategy::setCountStatement()
{
	_pCountStatement = new Statement(*_pSession);
}


inline Statement& ArchiveStrategy::getCopyStatement()
{
	return *_pCopyStatement;
}


inline Statement& ArchiveStrategy::getDeleteStatement()
{
	return *_pDeleteStatement;
}


inline Statement& ArchiveStrategy::getCountStatement()
{
	return *_pCountStatement;
}



//
// ArchiveByAgeStrategy
//
class Data_API ArchiveByAgeStrategy: public ArchiveStrategy
	/// Archives rows scheduled for archiving.
{
public:
	ArchiveByAgeStrategy(const std::string& connector, 
		const std::string& connect, 
		const std::string& sourceTable, 
		const std::string& destinationTable = DEFAULT_ARCHIVE_DESTINATION);
	
	~ArchiveByAgeStrategy();

	void archive();

	const std::string& getThreshold() const;
		/// Returns the archive threshold.

	void setThreshold(const std::string& threshold);
		/// Sets the archive threshold.

private:
	ArchiveByAgeStrategy();
	ArchiveByAgeStrategy(const ArchiveByAgeStrategy&);
	ArchiveByAgeStrategy& operator = (const ArchiveByAgeStrategy&);

	void initStatements();

	Timespan     _maxAge;
	std::string  _ageString;
	DateTime     _archiveDateTime;
	Poco::Dynamic::Var _archiveCount;
};


//
// inlines
//

inline const std::string& ArchiveByAgeStrategy::getThreshold() const
{
	return _ageString;
}


} } // namespace Poco::Data


#endif // Data_ArchiveStrategy_INCLUDED
