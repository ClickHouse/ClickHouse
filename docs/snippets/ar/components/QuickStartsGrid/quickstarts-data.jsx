export const quickStartsData = [
  {
    id: "connect-your-iceberg-catalog",
    title: "كيفية ربط Iceberg catalog الخاص بك في ClickHouse Cloud",
    description: "تعرّف على كيفية ربط ClickHouse Cloud بـ Data Catalog الخاص بك والاستعلام عن Iceberg tables.",
    href: "/get-started/quickstarts/connect-your-iceberg-catalog",
    useCases: ["data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-materialized-view",
    title: "أنشئ أول materialized view لك",
    description:
      "تعرّف على كيفية استخدام materialized views في ClickHouse لحساب نتائج الاستعلامات مسبقاً وتخزينها بترتيب فرز مختلف، مما يتيح عمليات بحث سريعة على الأعمدة غير المشمولة بـ primary key.",
    href: "/get-started/quickstarts/create-your-first-materialized-view",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-mergetree-table",
    title: "أنشئ أول MergeTree table لك",
    description:
      "تعرّف على آلية عمل محرك الجداول الرئيسي في ClickHouse من خلال إنشاء MergeTree table، وتحميل بيانات أسعار العقارات في المملكة المتحدة، ومراقبة تأثير الأجزاء وعمليات الدمج على التخزين وأداء الاستعلامات.",
    href: "/get-started/quickstarts/create-your-first-mergetree-table",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-projection",
    title: "أنشئ أول projection لك",
    description: "تعرّف على كيفية استخدام projections في ClickHouse لتخزين نسخة مرتّبة إضافية من بياناتك داخل الجدول نفسه، مما يتيح عمليات بحث سريعة على الأعمدة غير المشمولة بـ primary key.",
    href: "/get-started/quickstarts/create-your-first-projection",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud"]
  },
  {
    id: "create-your-first-service-on-cloud",
    title: "أنشئ أول Cloud service لك وحمّل بيانات تجريبية",
    description: "أنشئ ClickHouse Cloud service، واستكشف SQL Console، وحمّل مجموعة بيانات تجريبية لتبدأ الاستعلام عن بيانات حقيقية في غضون دقائق.",
    href: "/get-started/quickstarts/create-your-first-service-on-cloud",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "creating-tables",
    title: "إنشاء الجداول في ClickHouse",
    description: "تعرّف على كيفية إنشاء الجداول في ClickHouse",
    href: "/get-started/quickstarts/creating-tables",
    useCases: ["all"],
    products: ["self-managed"]
  },
  {
    id: "insert-data-using-clickhouse-client",
    title: "إدراج البيانات في ClickHouse Cloud باستخدام clickhouse-client",
    description: "تعرّف على كيفية استخدام clickhouse-client لإدراج البيانات من ملفات CSV وParquet المحلية في ClickHouse Cloud service عبر سطر الأوامر.",
    href: "/get-started/quickstarts/insert-data-using-clickhouse-client",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "mutations",
    title: "تحديث بيانات ClickHouse وحذفها",
    description: "يصف كيفية تنفيذ عمليات التحديث والحذف في ClickHouse",
    href: "/get-started/quickstarts/mutations",
    useCases: ["data-warehousing"],
    products: ["self-managed"]
  },
  {
    id: "obtain-your-cloud-connection-details",
    title: "احصل على تفاصيل الاتصال بـ Cloud الخاص بك",
    description: "تعرّف على كيفية العثور على hostname والمنفذ وبيانات الاعتماد لـ ClickHouse Cloud service الخاص بك للاتصال من خلال العملاء الخارجيين وأدوات CLI والتطبيقات.",
    href: "/get-started/quickstarts/obtain-your-cloud-connection-details",
    useCases: ["all"],
    products: ["cloud"]
  },
  {
    id: "tutorial",
    title: "دليل عملي متقدم",
    description: "تعرّف على كيفية استيعاب البيانات والاستعلام عنها في ClickHouse باستخدام مجموعة بيانات تجريبية لسيارات الأجرة في مدينة نيويورك.",
    href: "/get-started/quickstarts/tutorial",
    useCases: ["real-time-analytics", "data-warehousing"],
    products: ["cloud", "self-managed"]
  },
  {
    id: "working-with-the-map-type",
    title: "التعامل مع Map type في ClickHouse",
    description: "تعرّف على كيفية استخدام Map type في ClickHouse لتخزين بيانات key-value الديناميكية والاستعلام عنها وتجميعها، مع استخدام resource attributes الخاصة بـ OTel مثالاً عملياً.",
    href: "/get-started/quickstarts/working-with-the-map-type",
    useCases: ["observability"],
    products: ["self-managed"]
  },
  {
    id: "writing-queries",
    title: "الاستعلام عن بيانات ClickHouse",
    description: "تعرّف على كيفية الاستعلام عن بيانات ClickHouse",
    href: "/get-started/quickstarts/writing-queries",
    useCases: ["all"],
    products: ["self-managed"]
  }
]