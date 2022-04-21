#include <Parsers/MySQLCompatibility/ParserOverlay/ASTNode.h>

namespace MySQLCompatibility
{

using MySQLParserOverlay::ASTNode;
using MySQLParserOverlay::ASTNodePtr;

enum QueryType
{
	SET_QUERY_TYPE,
	UNKNOWN_QUERY_TYPE
};

using RecognizeResult = std::pair<ASTNodePtr, QueryType>;

class IRecognizer
{
public:
	virtual RecognizeResult Recognize(ASTNodePtr node) const = 0;
	virtual ~IRecognizer() {}
};

using IRecognizerPtr = std::shared_ptr<IRecognizer>;

class SetQueryRecognizer : public IRecognizer
{
public:
	virtual RecognizeResult Recognize(ASTNodePtr node) const override;
};

class GenericRecognizer : public IRecognizer
{
public:
	virtual RecognizeResult Recognize(ASTNodePtr node) const override;
private:
	std::vector<IRecognizerPtr> rules = {
		std::make_shared<SetQueryRecognizer>()
	};
};

}
