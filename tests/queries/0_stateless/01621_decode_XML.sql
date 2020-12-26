SELECT decodeXMLComponent('Hello, &quot;world&quot;!');
SELECT decodeXMLComponent('&lt;123&gt;');
SELECT decodeXMLComponent('&amp;clickhouse');
SELECT decodeXMLComponent('&apos;foo&apos;');