---
description: 'دليل لبناء ClickHouse من المصدر على أنظمة macOS'
sidebar_label: 'البناء على macOS لنظام macOS'
sidebar_position: 15
slug: /development/build-osx
title: 'البناء على macOS لنظام macOS'
keywords: ['MacOS', 'Mac', 'build']
doc_type: 'guide'
---

:::info هذا الدليل مخصص للمساهمين الذين يجرون تعديلات على ClickHouse نفسه.
إذا كنت لا تغيّر شيفرة مصدرية ClickHouse، فيمكنك تثبيت ClickHouse الجاهز كما هو موضح في [Quick Start](https://clickhouse.com/docs/get-started/quick-start).
:::

يمكن تجميع ClickHouse على macOS x86&#95;64 (Intel) وarm64 (Apple Silicon) باستخدام macOS 10.15 (Catalina) أو إصدار أحدث.

وفيما يتعلق بالمصرّف، لا يُدعَم سوى Clang من Homebrew.

<div id="install-prerequisites">
  ## تثبيت المتطلبات الأساسية
</div>

أولًا، راجع [توثيق المتطلبات الأساسية العامة](developer-instruction.md).

بعد ذلك، ثبّت [Homebrew](https://brew.sh/) ثم شغّل

ثم شغّل:

```bash
brew update
brew install ccache cmake ninja libtool gettext llvm lld binutils grep findutils nasm bash rust rustup
```

:::note
تستخدم Apple نظام ملفات غير حساس لحالة الأحرف افتراضيًا. ورغم أن هذا لا يؤثر عادةً في الترجمة البرمجية (خصوصًا أن عمليات البناء من الصفر ستنجح)، فقد يسبب التباسًا في عمليات الملفات مثل `git mv`.
لأعمال التطوير الجادة على macOS، تأكد من تخزين الشيفرة المصدرية على وحدة تخزين حساسة لحالة الأحرف؛ راجع مثلًا [هذه التعليمات](https://brianboyko.medium.com/a-case-sensitive-src-folder-for-mac-programmers-176cc82a3830).
:::

<div id="build-clickhouse">
  ## بناء ClickHouse
</div>

لبناء ClickHouse، يجب استخدام مصرّف Clang المرفق مع Homebrew:

```bash
cd ClickHouse
mkdir build
export PATH=$(brew --prefix llvm)/bin:$PATH
cmake -S . -B build
cmake --build build
# The resulting binary will be created at: build/programs/clickhouse
```

:::note
إذا واجهت أخطاء `ld: archive member '/' not a mach-o file in ...` أثناء عملية الربط، فقد تحتاج
إلى استخدام llvm-ar عبر تعيين الخيار `-DCMAKE_AR=/opt/homebrew/opt/llvm/bin/llvm-ar`.
:::

<div id="caveats">
  ## ملاحظات مهمة
</div>

إذا كنت تنوي تشغيل `clickhouse-server`، فتأكّد من زيادة قيمة `maxfiles` في النظام.

:::note
ستحتاج إلى استخدام sudo.
:::

للقيام بذلك، أنشئ الملف `/Library/LaunchDaemons/limit.maxfiles.plist` بالمحتوى التالي:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
        "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
  <dict>
    <key>Label</key>
    <string>limit.maxfiles</string>
    <key>ProgramArguments</key>
    <array>
      <string>launchctl</string>
      <string>limit</string>
      <string>maxfiles</string>
      <string>524288</string>
      <string>524288</string>
    </array>
    <key>RunAtLoad</key>
    <true/>
    <key>ServiceIPC</key>
    <false/>
  </dict>
</plist>
```

اضبط أذونات الملف بالشكل الصحيح:

```bash
sudo chown root:wheel /Library/LaunchDaemons/limit.maxfiles.plist
```

تأكّد من صحة الملف:

```bash
plutil /Library/LaunchDaemons/limit.maxfiles.plist
```

حمّل الملف (أو أعد التشغيل):

```bash
sudo launchctl load -w /Library/LaunchDaemons/limit.maxfiles.plist
```

للتحقق من أنه يعمل، استخدم الأمر `ulimit -n` أو `launchctl limit maxfiles`.