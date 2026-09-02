SELECT extractTextFromHTML('<script>Here is script.</script>');
SELECT extractTextFromHTML('<style>Here is style.</style>');
SELECT extractTextFromHTML('<![CDATA[Here is CDTATA.]]>');
SELECT extractTextFromHTML('This is a     white   space test.');
SELECT extractTextFromHTML('This is a complex test. <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"\n "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd"><html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en"><![CDATA[<script type="text/javascript">Hello, world</script> ]]><hello />world<![CDATA[ <style> ]]> hello</style>\n<script><![CDATA[</script>]]>hello</script>\n</html>');

DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
    stringColumn String
) ENGINE = Memory();

INSERT INTO defaults values ('<common tag>hello, world<tag>'), ('<script desc=content> some content </script>'), ('<![CDATA[hello, world]]>'), ('white space    collapse');

SELECT extractTextFromHTML(stringColumn) FROM defaults;
DROP table defaults;
