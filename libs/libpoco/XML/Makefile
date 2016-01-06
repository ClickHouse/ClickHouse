#
# Makefile
#
# $Id: //poco/1.4/XML/Makefile#1 $
#
# Makefile for Poco XML
#

include $(POCO_BASE)/build/rules/global

COMMONFLAGS += -DXML_NS -DXML_DTD -DHAVE_EXPAT_CONFIG_H

objects = AbstractContainerNode AbstractNode Attr AttrMap Attributes \
	AttributesImpl CDATASection CharacterData ChildNodesList Comment \
	ContentHandler DOMBuilder DOMException DOMImplementation DOMObject \
	DOMParser DOMSerializer DOMWriter DTDHandler DTDMap DeclHandler \
	DefaultHandler Document DocumentEvent DocumentFragment DocumentType \
	Element ElementsByTagNameList Entity EntityReference EntityResolver \
	EntityResolverImpl ErrorHandler Event EventDispatcher EventException \
	EventListener EventTarget InputSource LexicalHandler Locator LocatorImpl \
	MutationEvent Name NamePool NamedNodeMap NamespaceStrategy \
	NamespaceSupport Node NodeFilter NodeIterator NodeList Notation \
	ParserEngine ProcessingInstruction SAXException SAXParser Text \
	TreeWalker WhitespaceFilter XMLException XMLFilter XMLFilterImpl XMLReader \
	XMLString XMLWriter NodeAppender 

expat_objects = xmlparse xmlrole xmltok

ifdef POCO_UNBUNDLED
	SYSLIBS += -lexpat
else
	objects += $(expat_objects)
endif

target         = PocoXML
target_version = $(LIBVERSION)
target_libs    = PocoFoundation

include $(POCO_BASE)/build/rules/lib
