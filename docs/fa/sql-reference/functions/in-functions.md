---
machine_translated: true
machine_translated_rev: 72537a2d527c63c07aa5d2361a8829f3895cf2bd
toc_priority: 60
toc_title: "\u0627\u062C\u0631\u0627\u06CC \u0627\u067E\u0631\u0627\u062A\u0648\u0631\
  \ \u062F\u0631"
---

# توابع برای اجرای اپراتور در {#functions-for-implementing-the-in-operator}

## در notIn, globalIn, globalNotIn {#in-functions}

بخش را ببینید [در اپراتورها](../operators/in.md#select-in-operators).

## tuple(x, y, …), operator (x, y, …) {#tuplex-y-operator-x-y}

یک تابع است که اجازه می دهد تا گروه بندی ستون های متعدد.
For columns with the types T1, T2, …, it returns a Tuple(T1, T2, …) type tuple containing these columns. There is no cost to execute the function.
تاپل به طور معمول به عنوان مقادیر متوسط برای بحث در اپراتورها و یا برای ایجاد یک لیست از پارامترهای رسمی توابع لامبدا استفاده می شود. تاپل را نمی توان به یک جدول نوشته شده است.

## هشدار داده می شود {#tupleelementtuple-n-operator-x-n}

یک تابع است که اجازه می دهد تا گرفتن یک ستون از یک تاپل.
‘N’ شاخص ستون است, با شروع از 1. نفر باید ثابت باشد. ‘N’ باید ثابت باشه ‘N’ باید یک عدد صحیح انتخابی سخت نه بیشتر از اندازه تاپل باشد.
هیچ هزینه ای برای اجرای تابع وجود دارد.

[مقاله اصلی](https://clickhouse.tech/docs/en/query_language/functions/in_functions/) <!--hide-->
