SELECT decodeXMLComponent('Hello, &quot;world&quot;!');
SELECT decodeXMLComponent('&lt;123&gt;');
SELECT decodeXMLComponent('&amp;clickhouse');
SELECT decodeXMLComponent('&apos;foo&apos;');
SELECT decodeXMLComponent('Hello, &&amp; world');
SELECT decodeXMLComponent('Hello, &;&amp; world');
SELECT decodeXMLComponent('Hello, &a;&amp; world');
SELECT decodeXMLComponent('Hello, &ltt;&amp; world');
SELECT decodeXMLComponent('Hello, &ltt&amp; world');
SELECT decodeXMLComponent('Hello, &t;&amp; world');

--decode numeric entities

SELECT decodeXMLComponent('&#32;&#33;&#34;&#35;&#36;&#37;&#38;&#39;&#40;&#41;&#42;&#43;&#44;&#45;&#46;&#47;&#48;&#49;&#50;');
SELECT decodeXMLComponent('&#41;&#42;&#43;&#44;&#45;&#46;&#47;&#48;&#49;&#50;&#51;&#52;&#53;&#54;&#55;&#56;&#57;&#58;&#59;&#60;');
SELECT decodeXMLComponent('&#61;&#62;&#63;&#64;&#65;&#66;&#67;&#68;&#69;&#70;&#71;&#72;&#73;&#74;&#75;&#76;&#77;&#78;&#79;&#80;');
SELECT decodeXMLComponent('&#20026;');
SELECT decodeXMLComponent('&#x4e3a;');
SELECT decodeXMLComponent('&#12345678;&apos;123');
SELECT decodeXMLComponent('&#x0426;&#X0426;&#x042E;&#X042e;&#x042B;&#x3131;');