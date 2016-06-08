#pragma once

#include <sparsehash/dense_hash_map>

#include <Poco/File.h>
#include <Poco/NumberParser.h>
#include <Poco/Util/Application.h>
#include <Poco/Exception.h>

#include <common/Common.h>
#include <common/logger_useful.h>

#include <DB/Core/StringRef.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/ReadBufferFromFile.h>

/** @brief Класс, позволяющий узнавать по id региона его текстовое название на одном из поддерживаемых языков: ru, en, ua, by, kz, tr.
  *
  * Информацию об именах регионов загружает из текстовых файлов с названиями следующего формата:
  * 	regions_names_xx.txt,
  * где xx - одно из двух буквенных обозначений следующих поддерживаемых языков:
  * 	ru, en, ua, by, kz, tr.
  *
  * Умеет, по запросу, обновлять данные.
  */
class RegionsNames
{
public:
	enum class Language
	{
		RU = 0,
		EN,
		UA,
		BY,
		KZ,
		TR,
	};

private:
	static const size_t ROOT_LANGUAGE = 0;
	static const size_t SUPPORTED_LANGUAGES_COUNT = 6;
	static const size_t LANGUAGE_ALIASES_COUNT = 7;

	static const char ** getSupportedLanguages()
	{
		static const char * res[] { "ru", "en", "ua", "by", "kz", "tr" };
		return res;
	}

	struct language_alias { const char * const name; const Language lang; };
	static const language_alias * getLanguageAliases()
	{
		static constexpr const language_alias language_aliases[] {
			{ "ru", Language::RU },
			{ "en", Language::EN },
			{ "ua", Language::UA },
			{ "uk", Language::UA },
			{ "by", Language::BY },
			{ "kz", Language::KZ },
			{ "tr", Language::TR }
		};

		return language_aliases;
	}

	using RegionID_t = int;

	using Chars_t = std::vector<char>;
	using CharsForLanguageID_t = std::vector<Chars_t>;
	using ModificationTimes_t = std::vector<time_t>;
	using StringRefs_t = std::vector<StringRef>; /// Lookup table RegionID -> StringRef
	using StringRefsForLanguageID_t = std::vector<StringRefs_t>;

public:
	static constexpr auto required_key = "path_to_regions_names_files";

	RegionsNames(const std::string & directory_ = Poco::Util::Application::instance().config().getString(required_key))
		: directory(directory_)
	{
	}


	/** @brief Перезагружает, при необходимости, имена регионов.
	  */
	void reload()
	{
		LOG_DEBUG(log, "Reloading regions names");

		RegionID_t max_region_id = 0;
		for (size_t language_id = 0; language_id < SUPPORTED_LANGUAGES_COUNT; ++language_id)
		{
			const std::string & language = getSupportedLanguages()[language_id];
			std::string path = directory + "/regions_names_" + language + ".txt";

			Poco::File file(path);
			time_t new_modification_time = file.getLastModified().epochTime();
			if (new_modification_time <= file_modification_times[language_id])
				continue;
			file_modification_times[language_id] = new_modification_time;

			LOG_DEBUG(log, "Reloading regions names for language: " << language);

			DB::ReadBufferFromFile in(path);

			const size_t initial_size = 10000;

			Chars_t new_chars;
			StringRefs_t new_names_refs(initial_size, StringRef("", 0));

			/// Выделим непрерывный кусок памяти, которого хватит для хранения всех имён.
			new_chars.reserve(Poco::File(path).getSize());

			while (!in.eof())
			{
				RegionID_t region_id;
				std::string region_name;

				DB::readIntText(region_id, in);
				DB::assertChar('\t', in);
				DB::readString(region_name, in);
				DB::assertChar('\n', in);

				if (region_id <= 0)
					continue;

				size_t old_size = new_chars.size();

				if (new_chars.capacity() < old_size + region_name.length() + 1)
					throw Poco::Exception("Logical error. Maybe size of file " + path + " is wrong.");

				new_chars.resize(old_size + region_name.length() + 1);
				memcpy(&new_chars[old_size], region_name.c_str(), region_name.length() + 1);

				if (region_id > max_region_id)
					max_region_id = region_id;

				while (region_id >= static_cast<int>(new_names_refs.size()))
					new_names_refs.resize(new_names_refs.size() * 2, StringRef("", 0));

				new_names_refs[region_id] = StringRef(&new_chars[old_size], region_name.length());
			}

			chars[language_id].swap(new_chars);
			names_refs[language_id].swap(new_names_refs);
		}

		for (size_t language_id = 0; language_id < SUPPORTED_LANGUAGES_COUNT; ++language_id)
			names_refs[language_id].resize(max_region_id + 1, StringRef("", 0));
	}

	StringRef getRegionName(RegionID_t region_id, Language language = Language::RU) const
	{
		size_t language_id = static_cast<size_t>(language);

		if (static_cast<size_t>(region_id) > names_refs[language_id].size())
			return StringRef("", 0);

		StringRef ref = names_refs[language_id][region_id];

		while (ref.size == 0 && language_id != ROOT_LANGUAGE)
		{
			static const size_t FALLBACK[] = { 0, 0, 0, 0, 0, 1 };
			language_id = FALLBACK[language_id];
			ref = names_refs[language_id][region_id];
		}

		return ref;
	}

	static Language getLanguageEnum(const std::string & language)
	{
		if (language.size() == 2)
		{
			for (size_t i = 0; i < LANGUAGE_ALIASES_COUNT; ++i)
			{
				const auto & alias = getLanguageAliases()[i];
				if (language[0] == alias.name[0] && language[1] == alias.name[1])
					return alias.lang;
			}
		}
		throw Poco::Exception("Unsupported language for region name. Supported languages are: " + dumpSupportedLanguagesNames() + ".");
	}

	static std::string dumpSupportedLanguagesNames()
	{
		std::string res = "";
		for (size_t i = 0; i < LANGUAGE_ALIASES_COUNT; ++i)
		{
			if (i > 0)
				res += ", ";
			res += '\'';
			res += getLanguageAliases()[i].name;
			res += '\'';
		}
		return res;
	}

private:
	const std::string directory;
	ModificationTimes_t file_modification_times = ModificationTimes_t(SUPPORTED_LANGUAGES_COUNT);
	Logger * log = &Logger::get("RegionsNames");

	/// Байты имен для каждого языка, уложенные подряд, разделенные нулями
	CharsForLanguageID_t chars = CharsForLanguageID_t(SUPPORTED_LANGUAGES_COUNT);

	/// Отображение для каждого языка из id региона в указатель на диапазон байт имени
	StringRefsForLanguageID_t names_refs = StringRefsForLanguageID_t(SUPPORTED_LANGUAGES_COUNT);
};
