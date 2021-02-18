SELECT htmlOrXmlCoarseParse('<script>Here is script.</script>');
SELECT htmlOrXmlCoarseParse('<style>Here is style.</style>');
SELECT htmlOrXmlCoarseParse('<![CDATA[Here is CDTATA.]]>');
SELECT htmlOrXmlCoarseParse('This is a     white   space test.');
SELECT htmlOrXmlCoarseParse('This is a complex test. <!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Strict//EN"\n "http://www.w3.org/TR/xhtml1/DTD/xhtml1-strict.dtd"><html xmlns="http://www.w3.org/1999/xhtml" xml:lang="en" lang="en"><![CDATA[<script type="text/javascript">Hello, world</script> ]]><hello />world<![CDATA[ <style> ]]> hello</style>\n<script><![CDATA[</script>]]>hello</script>\n</html>');
DROP TABLE IF EXISTS defaults;
CREATE TABLE defaults
(
    stringColumn String
) ENGINE = Memory();

INSERT INTO defaults values ('<common tag>hello, world<tag>'), ('<script desc=content> some content </script>'), ('<![CDATA[hello, world]]>'), ('white space    collapse');

SELECT htmlOrXmlCoarseParse(stringColumn) FROM defaults;
DROP table defaults;
