select 1 format Template settings format_template_row='01070_nonexistent_file.txt'; -- { clientError 107 }
select 1 format Template settings format_template_row='/dev/null'; -- { clientError 474 }
