-- { echo }

SELECT extractTextFromHTML('');
SELECT extractTextFromHTML(' ');
SELECT extractTextFromHTML('  ');
SELECT extractTextFromHTML('Hello');
SELECT extractTextFromHTML('Hello, world');
SELECT extractTextFromHTML('Hello,  world');
SELECT extractTextFromHTML(' Hello,  world');
SELECT extractTextFromHTML(' Hello,  world ');
SELECT extractTextFromHTML(' \t Hello,\rworld \n ');

SELECT extractTextFromHTML('Hello<world');
SELECT extractTextFromHTML('Hello < world');
SELECT extractTextFromHTML('Hello > world');
SELECT extractTextFromHTML('Hello<world>');
SELECT extractTextFromHTML('Hello<>world');
SELECT extractTextFromHTML('Hello<!>world');
SELECT extractTextFromHTML('Hello<!->world');
SELECT extractTextFromHTML('Hello<!-->world');
SELECT extractTextFromHTML('Hello<!--->world');
SELECT extractTextFromHTML('Hello<!---->world');

SELECT extractTextFromHTML('Hello <!-- --> World');
SELECT extractTextFromHTML('Hello<!-- --> World');
SELECT extractTextFromHTML('Hello<!-- -->World');
SELECT extractTextFromHTML('Hello <!-- -->World');
SELECT extractTextFromHTML('Hello <u> World</u>');
SELECT extractTextFromHTML('Hello <u>World</u>');
SELECT extractTextFromHTML('Hello<u>World</u>');
SELECT extractTextFromHTML('Hello<u> World</u>');

SELECT extractTextFromHTML('<![CDATA[ \t Hello,\rworld \n ]]>');
SELECT extractTextFromHTML('Hello <![CDATA[Hello\tworld]]> world!');
SELECT extractTextFromHTML('Hello<![CDATA[Hello\tworld]]>world!');

SELECT extractTextFromHTML('Hello <![CDATA[Hello <b>world</b>]]> world!');
SELECT extractTextFromHTML('<![CDATA[<sender>John Smith</sender>]]>');
SELECT extractTextFromHTML('<![CDATA[<sender>John <![CDATA[Smith</sender>]]>');
SELECT extractTextFromHTML('<![CDATA[<sender>John <![CDATA[]]>Smith</sender>]]>');
SELECT extractTextFromHTML('<![CDATA[<sender>John ]]><![CDATA[Smith</sender>]]>');
SELECT extractTextFromHTML('<![CDATA[<sender>John ]]> <![CDATA[Smith</sender>]]>');
SELECT extractTextFromHTML('<![CDATA[<sender>John]]> <![CDATA[Smith</sender>]]>');
SELECT extractTextFromHTML('<![CDATA[<sender>John ]]>]]><![CDATA[Smith</sender>]]>');

SELECT extractTextFromHTML('Hello<script>World</script> goodbye');
SELECT extractTextFromHTML('Hello<script >World</script> goodbye');
SELECT extractTextFromHTML('Hello<scripta>World</scripta> goodbye');
SELECT extractTextFromHTML('Hello<script type="text/javascript">World</script> goodbye');
SELECT extractTextFromHTML('Hello<style type="text/css">World</style> goodbye');
SELECT extractTextFromHTML('Hello<script:p>World</script:p> goodbye');
SELECT extractTextFromHTML('Hello<script:p type="text/javascript">World</script:p> goodbye');

SELECT extractTextFromHTML('Hello<style type="text/css">World <!-- abc --> </style> goodbye');
SELECT extractTextFromHTML('Hello<style type="text/css">World <!-- abc --> </style \n > goodbye');
SELECT extractTextFromHTML('Hello<style type="text/css">World <!-- abc --> </ style> goodbye');
SELECT extractTextFromHTML('Hello<style type="text/css">World <!-- abc --> </stylea> goodbye');

SELECT extractTextFromHTML('Hello<style type="text/css">World <![CDATA[</style>]]> </stylea> goodbye');
SELECT extractTextFromHTML('Hello<style type="text/css">World <![CDATA[</style>]]> </style> goodbye');
SELECT extractTextFromHTML('Hello<style type="text/css">World <![CDAT[</style>]]> </style> goodbye');
SELECT extractTextFromHTML('Hello<style type="text/css">World <![endif]--> </style> goodbye');
SELECT extractTextFromHTML('Hello<style type="text/css">World <script>abc</script> </stylea> goodbye');
SELECT extractTextFromHTML('Hello<style type="text/css">World <script>abc</script> </style> goodbye');

SELECT extractTextFromHTML('<![CDATA[]]]]><![CDATA[>]]>');

SELECT extractTextFromHTML('
<img src="pictures/power.png" style="margin-bottom: -30px;" />
<br><span style="padding-right: 10px; font-size: 10px;">xkcd.com</span>
</div>
');
