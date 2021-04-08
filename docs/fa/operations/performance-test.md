---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 54
toc_title: "\u0633\u062E\u062A \u0627\u0641\u0632\u0627\u0631 \u062A\u0633\u062A"
---

# چگونه برای تست سخت افزار خود را با کلیک {#how-to-test-your-hardware-with-clickhouse}

با این آموزش شما می توانید اجرا پایه ClickHouse آزمون عملکرد بر روی هر سرور بدون نصب و راه اندازی ClickHouse بسته است.

1.  برو به “commits” صفحه: https://github.com/ClickHouse/ClickHouse/commits/master

2.  با کلیک بر روی اولین علامت چک سبز یا صلیب قرمز با سبز “ClickHouse Build Check” و با کلیک بر روی “Details” لینک نزدیک “ClickHouse Build Check”. چنین لینک در برخی از مرتکب وجود دارد, برای مثال مرتکب با اسناد و مدارک. در این مورد, را انتخاب کنید نزدیکترین ارتکاب داشتن این لینک.

3.  رونوشت از پیوند به “clickhouse” دودویی برای amd64 یا aarch64.

4.  به سرور بروید و با استفاده از ابزار دانلود کنید:

<!-- -->

      # For amd64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578163263_binary/clickhouse
      # For aarch64:
      wget https://clickhouse-builds.s3.yandex.net/0/00ba767f5d2a929394ea3be193b1f79074a1c4bc/1578161264_binary/clickhouse
      # Then do:
      chmod a+x clickhouse

1.  دانلود تنظیمات:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/users.xml
      mkdir config.d
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/path.xml -O config.d/path.xml
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/programs/server/config.d/log_to_console.xml -O config.d/log_to_console.xml

1.  دانلود فایل معیار:

<!-- -->

      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/benchmark-new.sh
      chmod a+x benchmark-new.sh
      wget https://raw.githubusercontent.com/ClickHouse/ClickHouse/master/benchmark/clickhouse/queries.sql

1.  دانلود داده های تست با توجه به [یاندکسمجموعه داده های متریکا](../getting-started/example-datasets/metrica.md) دستورالعمل (“hits” جدول حاوی 100 میلیون ردیف).

<!-- -->

      wget https://clickhouse-datasets.s3.yandex.net/hits/partitions/hits_100m_obfuscated_v1.tar.xz
      tar xvf hits_100m_obfuscated_v1.tar.xz -C .
      mv hits_100m_obfuscated_v1/* .

1.  اجرای کارساز:

<!-- -->

      ./clickhouse server

1.  داده ها را بررسی کنید: در ترمینال دیگر به سرور مراجعه کنید

<!-- -->

      ./clickhouse client --query "SELECT count() FROM hits_100m_obfuscated"
      100000000

1.  ویرایش benchmark-new.sh تغییر `clickhouse-client` به `./clickhouse client` و اضافه کردن `–-max_memory_usage 100000000000` پارامتر.

<!-- -->

      mcedit benchmark-new.sh

1.  اجرای معیار:

<!-- -->

      ./benchmark-new.sh hits_100m_obfuscated

1.  ارسال اعداد و اطلاعات در مورد پیکربندی سخت افزار خود را به clickhouse-feedback@yandex-team.com

همه نتایج در اینجا منتشر شده: https://clickhouse.فناوری / معیار / سخت افزار/
