SELECT '---SIMPLE PARENTNESS---';
SELECT extractXML(
    '<tag>simple</tag>',
    '/tag'
);
SELECT extractXML(
    '<tag><child>with child</child></tag>',
    '/tag/child'
);
SELECT extractXML(
    '<tag><child>first</child><child>second</child></tag>',
    '/tag/child'
);

SELECT '---BY INDEX---';
SELECT extractXML(
    '<tag><child>first</child><child>second</child></tag>',
    '/tag/child[2]'
);
SELECT extractXML(
    '<first><second><third>1</third><third>2</third><third>3</third></second><second><third>4</third><third>5</third><third>6</third></second></first>',
    '/first/second/third[1]'
);

SELECT '---TEXT---';
SELECT extractXML(
    '<tag><child>with child text</child></tag>',
    '/tag/child/text()'
);
SELECT extractXML(
    '<tag><child>first</child><child>second</child></tag>',
    '/tag/child/text()'
);
SELECT extractXML(
    '<tag><child>first</child><child>second</child></tag>',
    '/tag/child[2]/text()'
);
SELECT extractXML(
    '<first><second><third>1</third><third>2</third><third>3</third></second><second><third>4</third><third>5</third><third>6</third></second></first>',
    '/first/second/third[3]/text()'
);

SELECT '---WITH ATTRIBUTES---';
SELECT extractXML(
    '<tag attr="attribute">simple attribute</tag>',
    '/tag[@attr]'
);
SELECT extractXML(
    '<tag attr="attribute">simple attribute</tag>',
    '/tag[@attr="attribute"]'
);
SELECT extractXML(
    '<tag attr="attribute" second_attr>two attributes</tag>',
    '/tag[@second_attr]'
);
SELECT extractXML(
    '<tag attr="attribute" second_attr="second_attribute">two attributes</tag>',
    '/tag[@second_attr="second_attribute"]'
);
SELECT extractXML(
    '<tag attr><child attr>with child attribute</child></tag>',
    '/tag[@attr]/child[@attr]'
);
SELECT extractXML(
    '<tag attr><child id="1">with child attribute</child><child id="2">with child attribute</child></tag>',
    '/tag/child[@id="1"]'
);
SELECT extractXML(
    '<first><second><third id="1">1</third><third id="2">2</third><third id="3">3</third></second><second><third id="1">4</third><third id="2">5</third><third id="3">6</third></second></first>',
    '/first/second/third[@id="3"]/text()'
);
SELECT '---FULL---';
SELECT extractXML(
    '<first case="success"><second case="success"><third id="1" case="not_success">not_success</third><third id="2" case="success">success</third><third id="3" case="success">not_success</third></second><second case="success"><third id="4" case="success">success</third><third id="5" case="not_success">not_success</third><third id="6" case="success">not_success</third></second><second case="not_success"><third id="7" case="not_success">not_success</third><third id="8" case="not_success">not_success</third><third id="9" case="not_success">not_success</third></second></first>',
    '/first[@case="success"]/second[@case="success"]/third[@case="success"]/text()[1]'
);