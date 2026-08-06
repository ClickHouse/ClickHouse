import { galaxyOnClick } from '/snippets/lib/galaxy.jsx';

export const BetaBadge = ({ link, galaxyTrack, galaxyEvent }) => {
    if (link) {
        return (
            <a
                href={link}
                target="_blank"
                rel="noopener noreferrer"
                className="betaBadge"
                onClick={galaxyTrack && galaxyEvent ? galaxyOnClick(galaxyEvent) : undefined}
            >
                <span>베타</span>
            </a>
        )
    }

    return (
        <a
            href="/ko/reference/settings/beta-and-experimental-features#beta-features"
            className="betaBadge"
        >
            <span>베타 기능입니다</span>
        </a>
    )
};