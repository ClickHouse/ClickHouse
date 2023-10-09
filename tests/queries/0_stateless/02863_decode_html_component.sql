SELECT decodeHTMLComponent('Hello, &quot;world&quot;!');
SELECT decodeHTMLComponent('&lt;123&gt;');
SELECT decodeHTMLComponent('&amp;clickhouse');
SELECT decodeHTMLComponent('&apos;foo&apos;');
SELECT decodeHTMLComponent('Hello, &&amp; world');
SELECT decodeHTMLComponent('Hello, &;&amp; world');
SELECT decodeHTMLComponent('Hello, &a;&amp; world');
SELECT decodeHTMLComponent('Hello, &ltt;&amp; world');
SELECT decodeHTMLComponent('Hello, &ltt&amp; world');
SELECT decodeHTMLComponent('Hello, &t;&amp; world');

SELECT decodeHTMLComponent('&#32;&#33;&#34;&#35;&#36;&#37;&#38;&#39;&#40;&#41;&#42;&#43;&#44;&#45;&#46;&#47;&#48;&#49;&#50;');
SELECT decodeHTMLComponent('&#41;&#42;&#43;&#44;&#45;&#46;&#47;&#48;&#49;&#50;&#51;&#52;&#53;&#54;&#55;&#56;&#57;&#58;&#59;&#60;');
SELECT decodeHTMLComponent('&#61;&#62;&#63;&#64;&#65;&#66;&#67;&#68;&#69;&#70;&#71;&#72;&#73;&#74;&#75;&#76;&#77;&#78;&#79;&#80;');
SELECT decodeHTMLComponent('&#20026;');
SELECT decodeHTMLComponent('&#x4e3a;');
SELECT decodeHTMLComponent('&#12345678;&apos;123');
SELECT decodeHTMLComponent('&#x0426;&#X0426;&#x042E;&#X042e;&#x042B;&#x3131;');
SELECT decodeHTMLComponent('C&lscr;&iscr;&cscr;&kscr;&#x1d43b;&#x1d45c;&uscr;&sscr;&#x1d452;');
SELECT decodeHTMLComponent('C&lscr;&iscr;&cscr;&kscr;&#x1d43b;&#x1d45c;&uscr;&sscr;&#x1d452');
SELECT decodeHTMLComponent('C&lscr;&iscr;&cscr;&kscr;&#x1d43b;&#x1d45c;&uscr;&sscr;&#x1d452&#123;');
SELECT decodeHTMLComponent('');
SELECT decodeHTMLComponent('C');

