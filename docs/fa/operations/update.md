---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 47
toc_title: "\u0628\u0647 \u0631\u0648\u0632 \u0631\u0633\u0627\u0646\u06CC \u06A9\u0644\
  \u06CC\u06A9"
---

# به روز رسانی کلیک {#clickhouse-update}

اگر تاتر از بسته های دب نصب شد, اجرای دستورات زیر را بر روی سرور:

``` bash
$ sudo apt-get update
$ sudo apt-get install clickhouse-client clickhouse-server
$ sudo service clickhouse-server restart
```

اگر شما نصب تاتر با استفاده از چیزی غیر از بسته های دب توصیه می شود, استفاده از روش به روز رسانی مناسب.

کلیک می کند به روز رسانی توزیع را پشتیبانی نمی کند. این عملیات باید به صورت متوالی در هر سرور جداگانه انجام شود. هنوز تمام سرور بر روی یک خوشه به طور همزمان به روز رسانی نیست, یا خوشه برای برخی از زمان در دسترس نخواهد بود.
