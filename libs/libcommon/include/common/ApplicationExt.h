/**
 * @file
 * @author Sergey N. Yatskevich
 * @brief YANDEX_APP_MAIN macros
 */
/*
 * $Id$
 */
#ifndef __APPLICATION_EXT_H
#define __APPLICATION_EXT_H

#include <Poco/TextEncoding.h>
#include <Poco/Util/Application.h>

#define YANDEX_APP_MAIN(AppClassName) \
			int \
			main (int _argc, char* _argv[]) \
			{ \
				AppClassName app; \
				try \
				{ \
					app.init (_argc, _argv); \
					return app.run (); \
				} \
				catch (const Poco::Exception& _ex) \
				{ \
					app.logger ().log (_ex); \
				} \
				catch (const std::exception& _ex) \
				{ \
					app.logger ().error (Poco::Logger::format ("Got exception: $0", _ex.what ())); \
				} \
				catch (...) \
				{ \
					app.logger ().error ("Unknown exception"); \
				} \
				return Poco::Util::Application::EXIT_CONFIG; \
			}

#endif
