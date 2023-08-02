# Poco::XML (expat)

set (SRCS_EXPAT
    "${LIBRARY_DIR}/XML/src/xmlrole.c"
    "${LIBRARY_DIR}/XML/src/xmltok_impl.c"
    "${LIBRARY_DIR}/XML/src/xmltok_ns.c"
    "${LIBRARY_DIR}/XML/src/xmltok.c"
)

add_library (_poco_xml_expat ${SRCS_EXPAT})
add_library (Poco::XML::Expat ALIAS _poco_xml_expat)

target_include_directories (_poco_xml_expat PUBLIC "${LIBRARY_DIR}/XML/include")
target_include_directories (_poco_xml_expat PRIVATE "${LIBRARY_DIR}/Foundation/include")

# Poco::XML

set (SRCS
    "${LIBRARY_DIR}/XML/src/AbstractContainerNode.cpp"
    "${LIBRARY_DIR}/XML/src/AbstractNode.cpp"
    "${LIBRARY_DIR}/XML/src/Attr.cpp"
    "${LIBRARY_DIR}/XML/src/Attributes.cpp"
    "${LIBRARY_DIR}/XML/src/AttributesImpl.cpp"
    "${LIBRARY_DIR}/XML/src/AttrMap.cpp"
    "${LIBRARY_DIR}/XML/src/CDATASection.cpp"
    "${LIBRARY_DIR}/XML/src/CharacterData.cpp"
    "${LIBRARY_DIR}/XML/src/ChildNodesList.cpp"
    "${LIBRARY_DIR}/XML/src/Comment.cpp"
    "${LIBRARY_DIR}/XML/src/ContentHandler.cpp"
    "${LIBRARY_DIR}/XML/src/DeclHandler.cpp"
    "${LIBRARY_DIR}/XML/src/DefaultHandler.cpp"
    "${LIBRARY_DIR}/XML/src/Document.cpp"
    "${LIBRARY_DIR}/XML/src/DocumentEvent.cpp"
    "${LIBRARY_DIR}/XML/src/DocumentFragment.cpp"
    "${LIBRARY_DIR}/XML/src/DocumentType.cpp"
    "${LIBRARY_DIR}/XML/src/DOMBuilder.cpp"
    "${LIBRARY_DIR}/XML/src/DOMException.cpp"
    "${LIBRARY_DIR}/XML/src/DOMImplementation.cpp"
    "${LIBRARY_DIR}/XML/src/DOMObject.cpp"
    "${LIBRARY_DIR}/XML/src/DOMParser.cpp"
    "${LIBRARY_DIR}/XML/src/DOMSerializer.cpp"
    "${LIBRARY_DIR}/XML/src/DOMWriter.cpp"
    "${LIBRARY_DIR}/XML/src/DTDHandler.cpp"
    "${LIBRARY_DIR}/XML/src/DTDMap.cpp"
    "${LIBRARY_DIR}/XML/src/Element.cpp"
    "${LIBRARY_DIR}/XML/src/ElementsByTagNameList.cpp"
    "${LIBRARY_DIR}/XML/src/Entity.cpp"
    "${LIBRARY_DIR}/XML/src/EntityReference.cpp"
    "${LIBRARY_DIR}/XML/src/EntityResolver.cpp"
    "${LIBRARY_DIR}/XML/src/EntityResolverImpl.cpp"
    "${LIBRARY_DIR}/XML/src/ErrorHandler.cpp"
    "${LIBRARY_DIR}/XML/src/Event.cpp"
    "${LIBRARY_DIR}/XML/src/EventDispatcher.cpp"
    "${LIBRARY_DIR}/XML/src/EventException.cpp"
    "${LIBRARY_DIR}/XML/src/EventListener.cpp"
    "${LIBRARY_DIR}/XML/src/EventTarget.cpp"
    "${LIBRARY_DIR}/XML/src/InputSource.cpp"
    "${LIBRARY_DIR}/XML/src/LexicalHandler.cpp"
    "${LIBRARY_DIR}/XML/src/Locator.cpp"
    "${LIBRARY_DIR}/XML/src/LocatorImpl.cpp"
    "${LIBRARY_DIR}/XML/src/MutationEvent.cpp"
    "${LIBRARY_DIR}/XML/src/Name.cpp"
    "${LIBRARY_DIR}/XML/src/NamedNodeMap.cpp"
    "${LIBRARY_DIR}/XML/src/NamePool.cpp"
    "${LIBRARY_DIR}/XML/src/NamespaceStrategy.cpp"
    "${LIBRARY_DIR}/XML/src/NamespaceSupport.cpp"
    "${LIBRARY_DIR}/XML/src/Node.cpp"
    "${LIBRARY_DIR}/XML/src/NodeAppender.cpp"
    "${LIBRARY_DIR}/XML/src/NodeFilter.cpp"
    "${LIBRARY_DIR}/XML/src/NodeIterator.cpp"
    "${LIBRARY_DIR}/XML/src/NodeList.cpp"
    "${LIBRARY_DIR}/XML/src/Notation.cpp"
    "${LIBRARY_DIR}/XML/src/ParserEngine.cpp"
    "${LIBRARY_DIR}/XML/src/ProcessingInstruction.cpp"
    "${LIBRARY_DIR}/XML/src/QName.cpp"
    "${LIBRARY_DIR}/XML/src/SAXException.cpp"
    "${LIBRARY_DIR}/XML/src/SAXParser.cpp"
    "${LIBRARY_DIR}/XML/src/Text.cpp"
    "${LIBRARY_DIR}/XML/src/TreeWalker.cpp"
    "${LIBRARY_DIR}/XML/src/ValueTraits.cpp"
    "${LIBRARY_DIR}/XML/src/WhitespaceFilter.cpp"
    "${LIBRARY_DIR}/XML/src/XMLException.cpp"
    "${LIBRARY_DIR}/XML/src/XMLFilter.cpp"
    "${LIBRARY_DIR}/XML/src/XMLFilterImpl.cpp"
    "${LIBRARY_DIR}/XML/src/XMLReader.cpp"
    "${LIBRARY_DIR}/XML/src/XMLStreamParser.cpp"
    "${LIBRARY_DIR}/XML/src/XMLStreamParserException.cpp"
    "${LIBRARY_DIR}/XML/src/XMLString.cpp"
    "${LIBRARY_DIR}/XML/src/XMLWriter.cpp"

    # expat
    "${LIBRARY_DIR}/XML/src/xmlparse.cpp"
)

add_library (_poco_xml ${SRCS})
add_library (Poco::XML ALIAS _poco_xml)

target_compile_options (_poco_xml PRIVATE -Wno-old-style-cast)
target_include_directories (_poco_xml SYSTEM PUBLIC "${LIBRARY_DIR}/XML/include")
target_link_libraries (_poco_xml PUBLIC Poco::Foundation Poco::XML::Expat)
