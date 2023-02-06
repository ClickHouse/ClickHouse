//
// XMLTemplate.h
//


#ifndef PDF_XMLTemplate_INCLUDED
#define PDF_XMLTemplate_INCLUDED


#include "Poco/PDF/PDF.h"
#include "Poco/PDF/Document.h"
#include "Poco/Path.h"
#include <istream>


namespace Poco {
namespace PDF {


class PDF_API XMLTemplate
{
public:
	XMLTemplate(std::istream& xmlStream, const std::string& base = Poco::Path::current());
	XMLTemplate(const std::string& path);

	~XMLTemplate();

	void create(const std::string& fileName);

protected:
	void load(std::istream& xmlStream);

private:
	Poco::Path _base;
	Document* _pDocument;
};


} } // namespace Poco::PDF


#endif // PDF_DocumentTemplate_INCLUDED
