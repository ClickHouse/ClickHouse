solution "lzsse"
	configurations { "Debug", "Release" }
	platforms      { "x64" }
	includedirs    { "include" }
	flags          { "NoPCH" }
	location       ( _ACTION )
	configuration { "gmake" }
		buildoptions   { "-std=c++11", "-msse4.1" }
	
	project "lzsse"
		language "C++"
		kind "ConsoleApp"
		files { "lzsse2/*.cpp", "lzsse2/*.c", "lzsse2/*.h", "lzsse4/*.cpp", "lzsse4/*.c", "lzsse4/*.h", "lzsse8/*.cpp", "lzsse8/*.c", "lzsse8/*.h", "example/*.cpp", "example/*.c", "example/*.h" }

		configuration "Debug*"
			flags { "Symbols" }
			
		configuration "Release*"
			flags { "OptimizeSpeed" }

		configuration { "x64", "Debug" }
			targetdir "bin/64/debug"

		configuration { "x64", "Release" }
			targetdir "bin/64/release"
		