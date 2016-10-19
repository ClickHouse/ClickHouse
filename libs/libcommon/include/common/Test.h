#pragma once

#include <stdexcept>
#include <sstream>
#include <iostream>

/// Часть функциональности - только для сборки из репозитория Метрики.
#ifndef NO_METRIKA
	#include <ssqls/IDbObject.h>
#else
	struct IDbObject {};
#endif

#include <regex>
#include <iostream>

#include <DB/Common/StackTrace.h>
#include <DB/IO/WriteBufferFromString.h>

#include <Poco/ConsoleChannel.h>
#include <Poco/Logger.h>
#include <Poco/AutoPtr.h>


namespace Test
{
	template <class Result, class Reference, class Enable = void>
	struct Comparator
	{
		void check(const Result & result, const Reference & reference, const std::string & check_name = "")
		{
			if (result != reference)
			{
				std::stringstream ss;
				if (check_name.size())
					ss << "Check name: " << check_name << ". ";

				StackTrace stacktrace;
				ss << "Result ";
				if (check_name.size())
					ss << "for \"" << check_name << "\" ";

				ss << "differs from reference" <<
				". Result: " << result << ", reference: " << reference << "\n. Stacktrace: " << stacktrace.toString();
				throw std::logic_error(ss.str());
			}
		}
	};

	template <class Result, class Reference, class Enable = void, class ... Args>
	void compare(const Result & result, const Reference & reference, Args && ... args)
	{
		Comparator<Result, Reference> comp;
		comp.check(result, reference, std::forward<Args>(args)...);
	}

	template <class ContainerA, class ContainerB>
	void compareContainers(const ContainerA & result, const ContainerB & reference)
	{
		if (result != reference)
		{
			std::stringstream ss;
			ss << "Result differs from reference. Result: {";
			for (auto a : result)
				ss << " " << a;
			ss << " }, reference {";
			for (auto b : reference)
				ss << " " << b;
			ss << " }";

			StackTrace stacktrace;
			ss << "\n. Stacktrace: " << stacktrace.toString();

			throw std::logic_error(ss.str());
		}
	}

	template <class Result, class Reference>
	struct Comparator<Result, Reference, typename std::enable_if<
					std::is_base_of<IDbObject, Result>::value &&
					std::is_base_of<IDbObject, Reference>::value>::type>
	{
		void check(const Result & result, const Reference & reference, const std::string field_name_regexp_str = ".*")
		{
			std::string res_s;
			{
				DB::WriteBufferFromString res_b(res_s);
				result.writeTSKV(res_b, "", "", "");
			}

			std::string ref_s;
			{
				DB::WriteBufferFromString ref_b(ref_s);
				reference.writeTSKV(ref_b, "", "", "");
			}

			size_t ref_pos = 0;
			size_t res_pos = 0;

			while (ref_pos != std::string::npos && res_pos != std::string::npos)
			{
				size_t new_ref_pos = ref_s.find('\t', ref_pos);
				size_t new_res_pos = res_s.find('\t', res_pos);

				auto ref_field = ref_s.substr(ref_pos,
											  new_ref_pos != std::string::npos ? new_ref_pos - ref_pos : new_ref_pos);

				if (std::regex_match(ref_field, std::regex(field_name_regexp_str + "=.*")))
				{
					auto res_field = res_s.substr(res_pos, new_res_pos != std::string::npos ? new_res_pos - res_pos : new_res_pos);
					compare(res_field, ref_field);
				}

				res_pos = new_res_pos != res_s.size() && new_res_pos != std::string::npos ?
					new_res_pos + 1 : std::string::npos;
				ref_pos = new_ref_pos != ref_s.size() && new_ref_pos != std::string::npos ?
					new_ref_pos + 1 : std::string::npos;
			}
		}
	};

	inline void initLogger()
	{
		Poco::AutoPtr<Poco::ConsoleChannel> channel = new Poco::ConsoleChannel(std::cerr);
		Poco::Logger::root().setChannel(channel);
		Poco::Logger::root().setLevel("trace");
	}
};
