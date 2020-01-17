SELECT httpFunc('http://localhost:8123/?query=SELECT+', 'TSV',      'TSV', 'Int32', 1024) as result, toTypeName(result) as type
