/** Locale metadata shared by routes and the language switcher. */
export const LOCALE_METADATA = {
  en: { label: "English", hreflang: "en", dir: "ltr" },
  ar: { label: "العربية", hreflang: "ar", dir: "rtl" },
  es: { label: "Español", hreflang: "es", dir: "ltr" },
  fr: { label: "Français", hreflang: "fr", dir: "ltr" },
  ja: { label: "日本語", hreflang: "ja", dir: "ltr" },
  ko: { label: "한국어", hreflang: "ko", dir: "ltr" },
  "pt-br": { label: "Português (Brasil)", hreflang: "pt-BR", dir: "ltr" },
  ru: { label: "Русский", hreflang: "ru", dir: "ltr" },
  zh: { label: "中文", hreflang: "zh", dir: "ltr" },
} as const;

export type LocaleCode = keyof typeof LOCALE_METADATA;

export function localeInfo(locale: string) {
  return LOCALE_METADATA[locale.toLowerCase() as LocaleCode] ?? LOCALE_METADATA.en;
}

/** Convert a documentation path between English and a locale-prefixed route. */
export function localePath(pathname: string, locale: string): string {
  const normalized = pathname === "/" ? "/" : pathname.replace(/\/$/, "");
  const code = locale.toLowerCase();
  // English has no URL prefix. Be defensive about a stale client-side route
  // carrying `/en` during a language switch; `/docs/en` is not a real page.
  const documentPath = normalized === "/en" ? "/" : normalized.replace(/^\/en\//, "/");
  return code === "en" ? documentPath : `/${code}${documentPath === "/" ? "" : documentPath}`;
}
