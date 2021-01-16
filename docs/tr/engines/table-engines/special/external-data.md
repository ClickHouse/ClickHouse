---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 34
toc_title: "D\u0131\u015F veri"
---

# Sorgu işleme için harici veriler {#external-data-for-query-processing}

ClickHouse bir sunucu bir SELECT sorgusu ile birlikte bir sorgu işlemek için gerekli olan verileri gönderme sağlar. Bu veriler geçici bir tabloya konur (bölüme bakın “Temporary tables”) ve sorguda kullanılabilir (örneğin, işleçlerde).

Örneğin, önemli kullanıcı tanımlayıcılarına sahip bir metin dosyanız varsa, bu listeyi süzme kullanan bir sorgu ile birlikte sunucuya yükleyebilirsiniz.

Büyük hacimli dış verilerle birden fazla sorgu çalıştırmanız gerekiyorsa, bu özelliği kullanmayın. Verileri vaktinden önce DB'YE yüklemek daha iyidir.

Harici veriler komut satırı istemcisi (etkileşimli olmayan modda) veya HTTP arabirimi kullanılarak yüklenebilir.

Komut satırı istemcisinde, formatta bir parametreler bölümü belirtebilirsiniz

``` bash
--external --file=... [--name=...] [--format=...] [--types=...|--structure=...]
```

İletilen tablo sayısı için bunun gibi birden çok bölümünüz olabilir.

**–external** – Marks the beginning of a clause.
**–file** – Path to the file with the table dump, or -, which refers to stdin.
Stdın'den yalnızca tek bir tablo alınabilir.

Aşağıdaki parametreler isteğe bağlıdır: **–name**– Name of the table. If omitted, \_data is used.
**–format** – Data format in the file. If omitted, TabSeparated is used.

Aşağıdaki parametrelerden biri gereklidir:**–types** – A list of comma-separated column types. For example: `UInt64,String`. The columns will be named \_1, \_2, …
**–structure**– The table structure in the format`UserID UInt64`, `URL String`. Sütun adlarını ve türlerini tanımlar.

Belirtilen dosyalar ‘file’ belirtilen biçimde ayrıştırılır ‘format’, belirtilen veri türlerini kullanarak ‘types’ veya ‘structure’. Tablo sunucuya yüklenecek ve orada adı ile geçici bir tablo olarak erişilebilir ‘name’.

Örnekler:

``` bash
$ echo -ne "1\n2\n3\n" | clickhouse-client --query="SELECT count() FROM test.visits WHERE TraficSourceID IN _data" --external --file=- --types=Int8
849897
$ cat /etc/passwd | sed 's/:/\t/g' | clickhouse-client --query="SELECT shell, count() AS c FROM passwd GROUP BY shell ORDER BY c DESC" --external --file=- --name=passwd --structure='login String, unused String, uid UInt16, gid UInt16, comment String, home String, shell String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

HTTP arabirimini kullanırken, dış veriler çok parçalı/form veri biçiminde geçirilir. Her tablo ayrı bir dosya olarak iletilir. Tablo adı dosya adından alınır. Bu ‘query\_string’ parametreleri geçirilir ‘name\_format’, ‘name\_types’, ve ‘name\_structure’, nere ‘name’ bu parametreler karşılık gelen tablonun adıdır. Parametrelerin anlamı, komut satırı istemcisini kullanırken olduğu gibi aynıdır.

Örnek:

``` bash
$ cat /etc/passwd | sed 's/:/\t/g' > passwd.tsv

$ curl -F 'passwd=@passwd.tsv;' 'http://localhost:8123/?query=SELECT+shell,+count()+AS+c+FROM+passwd+GROUP+BY+shell+ORDER+BY+c+DESC&passwd_structure=login+String,+unused+String,+uid+UInt16,+gid+UInt16,+comment+String,+home+String,+shell+String'
/bin/sh 20
/bin/false      5
/bin/bash       4
/usr/sbin/nologin       1
/bin/sync       1
```

Dağıtılmış sorgu işleme için geçici tablolar tüm uzak sunuculara gönderilir.

[Orijinal makale](https://clickhouse.tech/docs/en/operations/table_engines/external_data/) <!--hide-->
