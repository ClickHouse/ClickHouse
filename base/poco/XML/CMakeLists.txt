# Poco::XML (expat)

set (SRCS_EXPAT
    src/xmlrole.c
    src/xmltok_impl.c
    src/xmltok_ns.c
    src/xmltok.c
)

add_library (_poco_xml_expat ${SRCS_EXPAT})
add_library (Poco::XML::Expat ALIAS _poco_xml_expat)

target_include_directories (_poco_xml_expat PUBLIC "include")
target_include_directories (_poco_xml_expat PRIVATE "../Foundation/include")

# Poco::XML

set (SRCS
    src/AbstractContainerNode.cpp
    src/AbstractNode.cpp
    src/Attr.cpp
    src/Attributes.cpp
    src/AttributesImpl.cpp
    src/AttrMap.cpp
    src/CDATASection.cpp
    src/CharacterData.cpp
    src/ChildNodesList.cpp
    src/Comment.cpp
    src/ContentHandler.cpp
    src/DeclHandler.cpp
    src/DefaultHandler.cpp
    src/Document.cpp
    src/DocumentEvent.cpp
    src/DocumentFragment.cpp
    src/DocumentType.cpp
    src/DOMBuilder.cpp
    src/DOMException.cpp
    src/DOMImplementation.cpp
    src/DOMObject.cpp
    src/DOMParser.cpp
    src/DOMSerializer.cpp
    src/DOMWriter.cpp
    src/DTDHandler.cpp
    src/DTDMap.cpp
    src/Element.cpp
    src/ElementsByTagNameList.cpp
    src/Entity.cpp
    src/EntityReference.cpp
    src/EntityResolver.cpp
    src/EntityResolverImpl.cpp
    src/ErrorHandler.cpp
    src/Event.cpp
    src/EventDispatcher.cpp
    src/EventException.cpp
    src/EventListener.cpp
    src/EventTarget.cpp
    src/InputSource.cpp
    src/LexicalHandler.cpp
    src/Locator.cpp
    src/LocatorImpl.cpp
    src/MutationEvent.cpp
    src/Name.cpp
    src/NamedNodeMap.cpp
    src/NamePool.cpp
    src/NamespaceStrategy.cpp
    src/NamespaceSupport.cpp
    src/Node.cpp
    src/NodeAppender.cpp
    src/NodeFilter.cpp
    src/NodeIterator.cpp
    src/NodeList.cpp
    src/Notation.cpp
    src/ParserEngine.cpp
    src/ProcessingInstruction.cpp
    src/QName.cpp
    src/SAXException.cpp
    src/SAXParser.cpp
    src/Text.cpp
    src/TreeWalker.cpp
    src/ValueTraits.cpp
    src/WhitespaceFilter.cpp
    src/XMLException.cpp
    src/XMLFilter.cpp
    src/XMLFilterImpl.cpp
    src/XMLReader.cpp
    src/XMLStreamParser.cpp
    src/XMLStreamParserException.cpp
    src/XMLString.cpp
    src/XMLWriter.cpp

    # expat
    src/xmlparse.cpp
)

add_library (_poco_xml ${SRCS})
add_library (Poco::XML ALIAS _poco_xml)

target_compile_options (_poco_xml PRIVATE -Wno-old-style-cast)
target_include_directories (_poco_xml SYSTEM PUBLIC "include")
target_link_libraries (_poco_xml PUBLIC Poco::Foundation Poco::XML::Expat)
