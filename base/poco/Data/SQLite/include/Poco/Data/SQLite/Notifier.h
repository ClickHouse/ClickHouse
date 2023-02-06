//
// Notifier.h
//
// Library: Data/SQLite
// Package: SQLite
// Module:  Notifier
//
// Definition of Notifier.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SQLite_Notifier_INCLUDED
#define SQLite_Notifier_INCLUDED


#include "Poco/Data/SQLite/SQLite.h"
#include "Poco/Data/SQLite/Utility.h"
#include "Poco/Data/Session.h"
#include "Poco/Mutex.h"
#include "Poco/Types.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/BasicEvent.h"
#include <map>


namespace Poco {
namespace Data {
namespace SQLite {


class SQLite_API Notifier
	/// Notifier is a wrapper for SQLite callback calls. It supports event callbacks
	/// for insert, update, delete, commit and rollback events. While (un)registering 
	/// callbacks is thread-safe, execution of the callbacks themselves are not; 
	/// it is the user's responsibility to ensure the thread-safey of the functions 
	/// they provide as callback target. Additionally, commit callbacks may prevent
	/// database transactions from succeeding (see sqliteCommitCallbackFn documentation
	/// for details). 
	/// 
	/// There can be only one set of callbacks per session (i.e. registering a new
	/// callback automatically unregisters the previous one). All callbacks are 
	/// registered and enabled at Notifier contruction time and can be disabled
	/// at a later point time.
{
public:
	typedef unsigned char EnabledEventType;
		/// A type definition for events-enabled bitmap.

	typedef Poco::BasicEvent<void> Event;

	// 
	// Events
	// 
	Event update;
	Event insert;
	Event erase;
	Event commit;
	Event rollback;

	// Event types.
	static const EnabledEventType SQLITE_NOTIFY_UPDATE   = 1;
	static const EnabledEventType SQLITE_NOTIFY_COMMIT   = 2;
	static const EnabledEventType SQLITE_NOTIFY_ROLLBACK = 4;

	Notifier(const Session& session,
		EnabledEventType enabled = SQLITE_NOTIFY_UPDATE | SQLITE_NOTIFY_COMMIT | SQLITE_NOTIFY_ROLLBACK);
			/// Creates a Notifier and enables all callbacks.

	Notifier(const Session& session,
		const Any& value,
		EnabledEventType enabled = SQLITE_NOTIFY_UPDATE | SQLITE_NOTIFY_COMMIT | SQLITE_NOTIFY_ROLLBACK);
			/// Creates a Notifier, assigns the value to the internal storage and and enables all callbacks.

	~Notifier();
		/// Disables all callbacks and destroys the Notifier.

	bool enableUpdate();
		/// Enables update callbacks.

	bool disableUpdate();
		/// Disables update callbacks.

	bool updateEnabled() const;
		/// Returns true if update callbacks are enabled, false otherwise.

	bool enableCommit();
		/// Enables commit callbacks.

	bool disableCommit();
		/// Disables commit callbacks.

	bool commitEnabled() const;
		/// Returns true if update callbacks are enabled, false otherwise.

	bool enableRollback();
		/// Enables rollback callbacks.

	bool disableRollback();
		/// Disables rollback callbacks.

	bool rollbackEnabled() const;
		/// Returns true if rollback callbacks are enabled, false otherwise.

	bool enableAll();
		/// Enables all callbacks.

	bool disableAll();
		/// Disables all callbacks.

	static void sqliteUpdateCallbackFn(void* pVal, int opCode, const char* pDB, const char* pTable, Poco::Int64 row);
		/// Update callback event dispatcher. Determines the type of the event, updates the row number 
		/// and triggers the event.

	static int sqliteCommitCallbackFn(void* pVal);
		/// Commit callback event dispatcher. If an exception occurs, it is catched inside this function,
		/// non-zero value is returned, which causes SQLite engine to turn commit into a rollback.
		/// Therefore, callers should check for return value - if it is zero, callback completed succesfuly
		/// and transaction was committed.

	static void sqliteRollbackCallbackFn(void* pVal);
		/// Rollback callback event dispatcher.

	bool operator == (const Notifier& other) const;
		/// Equality operator. Compares value, row and database handles and
		/// returns true iff all are equal.

	const std::string& getTable() const;
		/// Returns the table name.

	void setTable(const std::string& table);
		/// Sets the row number.

	Poco::Int64 getRow() const;
		/// Returns the row number.

	void setRow(Poco::Int64 row);
		/// Sets the row number.

	const Poco::Dynamic::Var& getValue() const;
		/// Returns the value.

	template <typename T>
	inline void setValue(const T& val)
		/// Sets the value.
	{
		_value = val;
	}

private:
	Notifier();
	Notifier(const Notifier&);
	Notifier& operator=(const Notifier&);

	const Session&     _session;
	std::string        _table;
	Poco::Int64        _row;
	Poco::Dynamic::Var _value;
	EnabledEventType   _enabledEvents;
	Poco::Mutex        _mutex;
};


// 
// inlines
// 

inline bool Notifier::operator == (const Notifier& other) const
{
	return _value == other._value &&
		_row == other._row &&
		Utility::dbHandle(_session) == Utility::dbHandle(other._session);
}


inline const std::string& Notifier::getTable() const
{
	return _table;
}


inline void Notifier::setTable(const std::string& table)
/// Sets the row number.
{
	_table = table;
}


inline Poco::Int64 Notifier::getRow() const
{
	return _row;
}


inline void Notifier::setRow(Poco::Int64 row)
{
	_row = row;
}


inline const Poco::Dynamic::Var& Notifier::getValue() const
{
	return _value;
}


} } } // namespace Poco::Data::SQLite


#endif // SQLite_Notifier_INCLUDED
