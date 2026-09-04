/**
 * No-op React hook shims for components defined inline in MDX pages
 * (`export const X = () => ...` using useState/useEffect). Those render
 * through Astro's MDX JSX runtime, never React, so hooks cannot run; the
 * shims yield the initial render and discard interactivity (accepted for
 * the hub pages, which are being rebuilt natively).
 */
export function useState<T>(initial: T | (() => T)): [T, (v: T) => void] {
  const value = typeof initial === "function" ? (initial as () => T)() : initial;
  return [value, () => {}];
}
export function useEffect(): void {}
export const useLayoutEffect = useEffect;
export function useRef<T>(initial: T | null = null): { current: T | null } {
  return { current: initial };
}
export function useMemo<T>(factory: () => T): T {
  return factory();
}
export function useCallback<T>(fn: T): T {
  return fn;
}
export function useId(): string {
  return "ch-ssr";
}
