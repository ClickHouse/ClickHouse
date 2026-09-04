import redirects from "@/generated/anchor-redirects.json";

type Redirect = { source: string; destination: string };

const destinations = new Map(
  (redirects as Redirect[]).map(({ source, destination }) => [
    source,
    destination,
  ]),
);

function currentPathWithFragment(): string {
  const path = `${window.location.pathname}${window.location.hash}`;
  try {
    return decodeURI(path);
  } catch {
    return path;
  }
}

function redirect(): void {
  const destination = destinations.get(currentPathWithFragment());
  if (!destination) return;
  const target = new URL(destination, window.location.origin);
  target.search = window.location.search;
  window.location.replace(`${target.pathname}${target.search}${target.hash}`);
}

redirect();
document.addEventListener("astro:page-load", redirect);
