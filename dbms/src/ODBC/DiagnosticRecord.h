#pragma once

#include <sql.h>
#include <stdexcept>

#include "Log.h"


struct DiagnosticRecord
{
	SQLINTEGER native_error_code = 0;
	std::string sql_state = "-----";
	std::string message;

	void fromException()
	{
		try
		{
			throw;
		}
		catch (const std::exception & e)
		{
			message = e.what();
			native_error_code = 1;
			sql_state = "HY000";	/// General error.
		}
		catch (...)
		{
			message = "Unknown exception.";
			native_error_code = 2;
			sql_state = "HY000";
		}

		LOG(message);
	}

	void reset()
	{
		native_error_code = 0;
		sql_state = "-----";
		message.clear();
	}
};
