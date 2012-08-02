#include <DB/Interpreters/Context.h>


namespace DB
{

String Context::getPath() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return shared->path;
}


void Context::setPath(const String & path)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	shared->path = path;
}


bool Context::isTableExist(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	Databases::const_iterator it;
	return shared->databases.end() != (it = shared->databases.find(db))
		&& it->second.end() != it->second.find(table_name);
}


bool Context::isDatabaseExist(const String & database_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	String db = database_name.empty() ? current_database : database_name;
	return shared->databases.end() != shared->databases.find(db);
}


void Context::assertTableExists(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;
		
	Databases::const_iterator it;
	if (shared->databases.end() == (it = shared->databases.find(db)))
		throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

	if (it->second.end() == it->second.find(table_name))
		throw Exception("Table " + db + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);
}


void Context::assertTableDoesntExist(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	Databases::const_iterator it;
	if (shared->databases.end() != (it = shared->databases.find(db))
		&& it->second.end() != it->second.find(table_name))
		throw Exception("Table " + db + "." + table_name + " already exists.", ErrorCodes::TABLE_ALREADY_EXISTS);
}


void Context::assertDatabaseExists(const String & database_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	if (shared->databases.end() == shared->databases.find(db))
		throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);
}


void Context::assertDatabaseDoesntExist(const String & database_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	if (shared->databases.end() != shared->databases.find(db))
		throw Exception("Database " + db + " already exists.", ErrorCodes::DATABASE_ALREADY_EXISTS);
}


StoragePtr Context::getTable(const String & database_name, const String & table_name) const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	Databases::const_iterator it;
	if (shared->databases.end() == (it = shared->databases.find(db)))
		throw Exception("Database " + db + " doesn't exist", ErrorCodes::UNKNOWN_DATABASE);

	Tables::const_iterator jt;
	if (it->second.end() == (jt = it->second.find(table_name)))
		throw Exception("Table " + db + "." + table_name + " doesn't exist.", ErrorCodes::UNKNOWN_TABLE);

	return jt->second;
}


void Context::addTable(const String & database_name, const String & table_name, StoragePtr table)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	
	String db = database_name.empty() ? current_database : database_name;

	assertDatabaseExists(db);
	assertTableDoesntExist(db, table_name);
	shared->databases[db][table_name] = table;
}


void Context::addDatabase(const String & database_name)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	assertDatabaseDoesntExist(db);
	shared->databases[db];
}


void Context::detachTable(const String & database_name, const String & table_name)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	assertTableExists(db, table_name);
	shared->databases[db].erase(shared->databases[db].find(table_name));
}


void Context::detachDatabase(const String & database_name)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);

	String db = database_name.empty() ? current_database : database_name;

	assertDatabaseExists(db);
	shared->databases.erase(shared->databases.find(db));
}


Settings Context::getSettings() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return settings;
}


void Context::setSettings(const Settings & settings_)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	settings = settings_;
}


String Context::getCurrentDatabase() const
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	return current_database;
}


void Context::setCurrentDatabase(const String & name)
{
	Poco::ScopedLock<Poco::Mutex> lock(shared->mutex);
	assertDatabaseExists(name);
	current_database = name;
}


Context & Context::getSessionContext()
{
	if (!session_context)
		throw Exception("There is no session", ErrorCodes::THERE_IS_NO_SESSION);
	return *session_context;
}


Context & Context::getGlobalContext()
{
	if (!global_context)
		throw Exception("Logical error: there is no global context", ErrorCodes::LOGICAL_ERROR);
	return *global_context;
}

}
