//
// Template.h
//
// Library: JSON
// Package: JSON
// Module:  Template
//
// Definition of the Template class.
//
// Copyright (c) 2012, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef JSON_JSONTemplate_INCLUDED
#define JSON_JSONTemplate_INCLUDED


#include "Poco/JSON/JSON.h"
#include "Poco/Dynamic/Var.h"
#include "Poco/SharedPtr.h"
#include "Poco/Path.h"
#include "Poco/Timestamp.h"
#include <sstream>
#include <stack>


namespace Poco {
namespace JSON {


class MultiPart;


POCO_DECLARE_EXCEPTION(JSON_API, JSONTemplateException, Poco::Exception)


class JSON_API Template
	/// Template is a template engine which uses JSON as input
	/// for generating output. There are commands for
	/// looping over JSON arrays, include other templates,
	/// conditional output, etc.
	///
	/// All text is send to the outputstream. A command is placed
	/// between 
	///     <?
	/// and 
	///     ?>
	/// ----
	///
	/// These are the available commands:
	/// 
	///     <? echo query ?>
	/// ----
	/// The result of the query is send to the output stream
	/// This command can also be written as <?= query ?>
	/// 
	///     <? if query ?> <? else ?> <? endif ?>
	/// ----
	/// When the result of query is true, all the text between
	/// if and else (or endif when there is no else) is send to the
	/// output stream. When the result of query is false, all the text
	/// between else and endif is send to the output stream. An empty
	/// object, an empty array or a null value is considered as a false value.
	/// For numbers a zero is false. An empty String is also false.
	///
	///     <? ifexist query ?> <? else ?> <? endif ?>
	/// ----
	/// This can be used to check the existence of the value.
	/// Use this for example when a zero value is ok (which returns false for <? if ?>.
	/// 
	///     <? for variable query ?> <? endfor ?>
	/// ----
	/// The result of the query must be an array. For each element
	/// in the array the text between for and endfor is send to the
	/// output stream. The active element is stored in the variable.
	///
	///     <? include "filename" ?>
	/// ----
	/// Includes a template. When the filename is relative it will try
	/// to resolve the filename against the active template. When this
	/// file doesn't exist, it can still be found when the JSONTemplateCache
	/// is used.
	///
	///  A query is passed to Poco::JSON::Query to get the value.
{
public:
	typedef SharedPtr<Template> Ptr;

	Template();
		/// Creates a Template.

	Template(const Path& templatePath);
		/// Creates a Template from the file with the given templatePath.

	virtual ~Template();
		/// Destroys the Template.

	void parse();
		/// Parse a template from a file.

	void parse(const std::string& source);
		/// Parse a template from a string.

	void parse(std::istream& in);
		/// Parse a template from an input stream.

	Timestamp parseTime() const;
		/// Returns the time when the template was parsed.

	void render(const Dynamic::Var& data, std::ostream& out) const;
		/// Renders the template and send the output to the stream.

private:
	std::string readText(std::istream& in);
	std::string readWord(std::istream& in);
	std::string readQuery(std::istream& in);
	std::string readTemplateCommand(std::istream& in);
	std::string readString(std::istream& in);
	void readWhiteSpace(std::istream& in);

	MultiPart* _parts;
	std::stack<MultiPart*> _partStack;
	MultiPart* _currentPart;
	Path _templatePath;
	Timestamp _parseTime;
};


//
// inlines
//
inline void Template::parse(const std::string& source)
{
	std::istringstream is(source);
	parse(is);
}


inline Timestamp Template::parseTime() const
{
	return _parseTime;
}


} } // namespace Poco::JSON


#endif // JSON_JSONTemplate_INCLUDED
