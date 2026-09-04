import { type ClassValue, clsx } from "clsx";
import { twMerge } from "tailwind-merge";

/** Compose class names with Tailwind conflict resolution. Consumer class (last arg) wins. */
export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs));
}
